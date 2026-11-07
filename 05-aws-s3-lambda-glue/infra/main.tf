terraform {
  required_version = ">= 1.6"
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 5.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.4" }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  type = string
}

data "aws_caller_identity" "current" {}

locals {
  database = "retail"
  table    = "orders"
}

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_glue_catalog_database" "retail" {
  name = local.database
}

resource "aws_glue_catalog_table" "orders" {
  name          = local.table
  database_name = aws_glue_catalog_database.retail.name
  table_type    = "EXTERNAL_TABLE"

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/curated/orders/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {
      name = "order_id"
      type = "bigint"
    }
    columns {
      name = "store_id"
      type = "bigint"
    }
    columns {
      name = "item_id"
      type = "bigint"
    }
    columns {
      name = "quantity"
      type = "bigint"
    }
  }
}

resource "aws_iam_role" "lambda" {
  name = "orders-ingest-lambda"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = templatefile("${path.module}/iam_policy.json", {
    bucket     = aws_s3_bucket.lake.bucket
    region     = var.region
    account_id = data.aws_caller_identity.current.account_id
    database   = local.database
    table      = local.table
  })
}

data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../ingest"
  output_path = "${path.module}/build/ingest.zip"
}

resource "aws_lambda_function" "ingest" {
  function_name    = "orders-ingest"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 60
  environment {
    variables = { GLUE_DATABASE = local.database, GLUE_TABLE = local.table }
  }
}

resource "aws_lambda_permission" "s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.lake.arn
}

resource "aws_s3_bucket_notification" "landing" {
  bucket = aws_s3_bucket.lake.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/orders/"
    filter_suffix       = ".jsonl"
  }
  depends_on = [aws_lambda_permission.s3]
}
