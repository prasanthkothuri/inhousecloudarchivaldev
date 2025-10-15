#!/usr/bin/env python3
"""
Utility script to reconcile AWS Lake Formation governance for archived datasets.

The script is intentionally idempotent so it can be re-run safely as archive
locations or consumer roles evolve. It performs the following optional actions:

1. Register the archive S3 location with Lake Formation.
2. Ensure specified IAM principals are data lake administrators.
3. Create/update an optional LF-tag and attach it to the archive database/tables.
4. Grant data access permissions to a consumer IAM role (database- or table-level).

Example:
    scripts/manage_lakeformation_governance.py \\
        --bucket my-archive-bucket \\
        --prefix warehouse/dev \\
        --glue-database archive_sales \\
        --consumer-role-arn arn:aws:iam::123456789012:role/analytics-readonly \\
        --lf-tag-key archive \\
        --lf-tag-values pii \\
        --tables orders customers

Defaults:
    --region eu-west-1
    --permissions ALL
    --tables (omitted) -> resolved to all tables within the Glue database.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import boto3
from botocore.exceptions import ClientError


LOGGER = logging.getLogger("lakeformation-governance")


@dataclass(frozen=True)
class Arguments:
    region: str
    bucket: str
    prefix: Optional[str]
    glue_database: str
    tables: Optional[Sequence[str]]
    consumer_role_arn: Optional[str]
    permissions: Sequence[str]
    data_lake_admin_arns: Sequence[str]
    lf_tag_key: Optional[str]
    lf_tag_values: Sequence[str]
    use_service_linked_role: bool
    resource_role_arn: Optional[str]


def parse_args(argv: Optional[Sequence[str]] = None) -> Arguments:
    parser = argparse.ArgumentParser(description="Manage Lake Formation governance for archive tables.")
    parser.add_argument(
        "--region",
        default="eu-west-1",
        help="AWS region for the Lake Formation/Glue APIs. Defaults to eu-west-1.",
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket containing the archived data.")
    parser.add_argument(
        "--prefix",
        help="Optional S3 prefix within the bucket to scope the Lake Formation resource.",
    )
    parser.add_argument(
        "--glue-database",
        required=True,
        help="Glue Data Catalog database name that stores the archived tables.",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        help="Specific Glue tables to operate on. Defaults to all tables in the database.",
    )
    parser.add_argument(
        "--consumer-role-arn",
        help="IAM role ARN that should receive Lake Formation permissions. If omitted, grants are skipped.",
    )
    parser.add_argument(
        "--permissions",
        nargs="+",
        default=["ALL"],
        help="Lake Formation permissions to grant to the consumer role. Defaults to ALL.",
    )
    parser.add_argument(
        "--data-lake-admin-arns",
        nargs="*",
        default=(),
        help="IAM principal ARNs that must be Lake Formation data lake administrators.",
    )
    parser.add_argument(
        "--lf-tag-key",
        help="Optional LF-tag key to enforce on the archive database/tables.",
    )
    parser.add_argument(
        "--lf-tag-values",
        nargs="*",
        default=(),
        help="LF-tag values to ensure exist for the provided key.",
    )
    parser.add_argument(
        "--resource-role-arn",
        help="Optional IAM role ARN that Lake Formation should assume for data access to the S3 location.",
    )
    parser.add_argument(
        "--use-service-linked-role",
        action="store_true",
        help="Use the Lake Formation service-linked role when registering the S3 location.",
    )

    args = parser.parse_args(argv)

    if args.lf_tag_key and not args.lf_tag_values:
        parser.error("--lf-tag-values is required when --lf-tag-key is supplied.")

    if args.use_service_linked_role and args.resource_role_arn:
        parser.error("Use either --use-service-linked-role or --resource-role-arn, not both.")

    if not args.consumer_role_arn:
        LOGGER.info("No consumer role provided; permission grant step will be skipped.")

    if args.tables and args.consumer_role_arn is None and not args.lf_tag_key:
        LOGGER.warning("Tables were provided but no consumer role; table-level grants will not be executed.")

    return Arguments(
        region=args.region,
        bucket=args.bucket,
        prefix=args.prefix,
        glue_database=args.glue_database,
        tables=args.tables,
        consumer_role_arn=args.consumer_role_arn,
        permissions=args.permissions,
        data_lake_admin_arns=args.data_lake_admin_arns,
        lf_tag_key=args.lf_tag_key,
        lf_tag_values=args.lf_tag_values,
        use_service_linked_role=args.use_service_linked_role,
        resource_role_arn=args.resource_role_arn,
    )


def build_s3_resource_arn(bucket: str, prefix: Optional[str]) -> str:
    if prefix:
        normalized_prefix = prefix.strip("/")
        return f"arn:aws:s3:::{bucket}/{normalized_prefix}"
    return f"arn:aws:s3:::{bucket}"


def list_tables(glue_client, database_name: str) -> List[str]:
    LOGGER.info("Resolving tables for Glue database %s.", database_name)
    paginator = glue_client.get_paginator("get_tables")
    names: List[str] = []
    for page in paginator.paginate(DatabaseName=database_name):
        for table in page.get("TableList", []):
            names.append(table["Name"])
    LOGGER.info("Discovered %d table(s) in Glue database %s.", len(names), database_name)
    return names


def ensure_resource_registered(
    lakeformation_client,
    resource_arn: str,
    assume_role_arn: Optional[str],
    use_service_linked_role: bool,
) -> None:
    LOGGER.info("Ensuring Lake Formation resource is registered: %s", resource_arn)
    try:
        lakeformation_client.describe_resource(ResourceArn=resource_arn)
        LOGGER.info("Resource already registered; skipping.")
        return
    except lakeformation_client.exceptions.EntityNotFoundException:
        LOGGER.info("Resource not yet registered; proceeding.")

    register_kwargs = {"ResourceArn": resource_arn}
    if assume_role_arn:
        register_kwargs["RoleArn"] = assume_role_arn
    else:
        register_kwargs["UseServiceLinkedRole"] = use_service_linked_role or True

    lakeformation_client.register_resource(**register_kwargs)
    LOGGER.info("Successfully registered resource.")


def ensure_data_lake_admins(lakeformation_client, admin_arns: Iterable[str]) -> None:
    admins = list(admin_arns)
    if not admins:
        return

    LOGGER.info("Ensuring data lake admins are configured.")
    settings = lakeformation_client.get_data_lake_settings()
    current_admins = {
        entry["DataLakePrincipalIdentifier"] for entry in settings.get("DataLakeAdmins", [])
    }
    missing = [arn for arn in admins if arn not in current_admins]
    if not missing:
        LOGGER.info("All requested data lake admins are already configured.")
        return

    updated_admins = list(settings.get("DataLakeAdmins", []))
    updated_admins.extend({"DataLakePrincipalIdentifier": arn} for arn in missing)
    lakeformation_client.put_data_lake_settings(DataLakeSettings={"DataLakeAdmins": updated_admins})
    LOGGER.info("Added %d data lake admin(s).", len(missing))


def ensure_lf_tag(
    lakeformation_client,
    tag_key: str,
    tag_values: Iterable[str],
) -> None:
    values = list(tag_values)
    if not values:
        return

    LOGGER.info("Ensuring LF-tag %s exists with required values.", tag_key)
    try:
        response = lakeformation_client.get_lf_tag(TagKey=tag_key)
        current_values = set(response.get("TagValues", []))
        missing = [value for value in values if value not in current_values]
        if missing:
            lakeformation_client.update_lf_tag(
                TagKey=tag_key,
                TagValuesToAdd=missing,
            )
            LOGGER.info("Added missing LF-tag values: %s", ", ".join(missing))
    except lakeformation_client.exceptions.EntityNotFoundException:
        lakeformation_client.create_lf_tag(
            TagKey=tag_key,
            TagValues=list(values),
        )
        LOGGER.info("Created LF-tag %s.", tag_key)


def attach_lf_tag_to_database(
    lakeformation_client,
    catalog_id: Optional[str],
    database_name: str,
    tag_key: str,
    tag_values: Iterable[str],
) -> None:
    expression = [
        {"TagKey": tag_key, "TagValues": list(tag_values)},
    ]
    resource = {"Database": {"Name": database_name}}
    if catalog_id:
        resource["Database"]["CatalogId"] = catalog_id

    existing_tags = lakeformation_client.get_resource_lf_tags(Resource=resource).get("LFTagOnDatabase", [])
    if any(entry["TagKey"] == tag_key for entry in existing_tags):
        LOGGER.info("Database %s already tagged with %s.", database_name, tag_key)
        return

    lakeformation_client.add_lf_tags_to_resource(Resource=resource, LFTags=expression)
    LOGGER.info("Attached LF-tag %s to database %s.", tag_key, database_name)


def attach_lf_tag_to_table(
    lakeformation_client,
    catalog_id: Optional[str],
    database_name: str,
    table_name: str,
    tag_key: str,
    tag_values: Iterable[str],
) -> None:
    expression = [
        {"TagKey": tag_key, "TagValues": list(tag_values)},
    ]
    table_resource = {"DatabaseName": database_name, "Name": table_name}
    if catalog_id:
        table_resource["CatalogId"] = catalog_id

    resource = {"Table": table_resource}
    current_tags = lakeformation_client.get_resource_lf_tags(Resource=resource).get("LFTagsOnTable", [])
    if any(entry["TagKey"] == tag_key for entry in current_tags):
        LOGGER.info("Table %s already tagged with %s.", table_name, tag_key)
        return

    lakeformation_client.add_lf_tags_to_resource(Resource=resource, LFTags=expression)
    LOGGER.info("Attached LF-tag %s to table %s.", tag_key, table_name)


def grant_permissions(
    lakeformation_client,
    principal_arn: str,
    permissions: Iterable[str],
    resource: dict,
) -> None:
    existing = lakeformation_client.list_permissions(
        Principal={"DataLakePrincipalIdentifier": principal_arn},
        Resource=resource,
    )

    desired_permissions = set(permissions)

    for entry in existing.get("PrincipalResourcePermissions", []):
        if entry["Resource"] == resource:
            current_perm = set(entry.get("Permissions", []))
            if desired_permissions.issubset(current_perm):
                LOGGER.info("Principal already has permissions on resource; skipping grant.")
                return

    lakeformation_client.grant_permissions(
        Principal={"DataLakePrincipalIdentifier": principal_arn},
        Resource=resource,
        Permissions=list(desired_permissions),
        PermissionsWithGrantOption=[],
    )
    LOGGER.info("Granted permissions %s to %s.", ", ".join(desired_permissions), principal_arn)


def main(argv: Optional[Sequence[str]] = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s - %(message)s",
    )

    args = parse_args(argv)

    session = boto3.Session(region_name=args.region)
    lakeformation = session.client("lakeformation")
    glue = session.client("glue")

    try:
        account_id = session.client("sts").get_caller_identity()["Account"]
    except ClientError as exc:
        LOGGER.error("Unable to determine AWS account ID: %s", exc)
        return 1

    glue_catalog_id = account_id

    resource_arn = build_s3_resource_arn(args.bucket, args.prefix)

    try:
        ensure_resource_registered(
            lakeformation_client=lakeformation,
            resource_arn=resource_arn,
            assume_role_arn=args.resource_role_arn,
            use_service_linked_role=args.use_service_linked_role,
        )
        ensure_data_lake_admins(lakeformation, args.data_lake_admin_arns)

        tables_specified = args.tables is not None
        resolved_tables: List[str] = []
        if tables_specified:
            resolved_tables = list(args.tables or [])
        elif args.lf_tag_key or args.consumer_role_arn:
            resolved_tables = list_tables(glue, args.glue_database)

        if args.lf_tag_key:
            ensure_lf_tag(lakeformation, args.lf_tag_key, args.lf_tag_values)
            attach_lf_tag_to_database(
                lakeformation_client=lakeformation,
                catalog_id=glue_catalog_id,
                database_name=args.glue_database,
                tag_key=args.lf_tag_key,
                tag_values=args.lf_tag_values,
            )
            for table in resolved_tables:
                attach_lf_tag_to_table(
                    lakeformation_client=lakeformation,
                    catalog_id=glue_catalog_id,
                    database_name=args.glue_database,
                    table_name=table,
                    tag_key=args.lf_tag_key,
                    tag_values=args.lf_tag_values,
                )

        if args.consumer_role_arn:
            if not tables_specified:
                resource = {
                    "Database": {
                        "CatalogId": glue_catalog_id,
                        "Name": args.glue_database,
                    }
                }
                grant_permissions(
                    lakeformation_client=lakeformation,
                    principal_arn=args.consumer_role_arn,
                    permissions=args.permissions,
                    resource=resource,
                )
                if not resolved_tables:
                    resolved_tables = list_tables(glue, args.glue_database)

            for table in resolved_tables:
                resource = {
                    "Table": {
                        "CatalogId": glue_catalog_id,
                        "DatabaseName": args.glue_database,
                        "Name": table,
                    }
                }
                grant_permissions(
                    lakeformation_client=lakeformation,
                    principal_arn=args.consumer_role_arn,
                    permissions=args.permissions,
                    resource=resource,
                )

    except ClientError as exc:
        LOGGER.error("AWS API call failed: %s", exc, exc_info=True)
        return 1

    LOGGER.info("Lake Formation governance reconciliation complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
