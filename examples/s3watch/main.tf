terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {}

locals {
  sqs_queue_prefix = "ice-s3watch"
  s3_bucket_prefix = "ice-s3watch"
}

resource "aws_s3_bucket" "this" {
  bucket_prefix = local.s3_bucket_prefix
  force_destroy = true
}

resource "aws_sqs_queue" "this" {
  name_prefix = local.sqs_queue_prefix
}

resource "aws_sqs_queue_policy" "this" {
  queue_url = aws_sqs_queue.this.id
  policy    = data.aws_iam_policy_document.queue.json
}

data "aws_iam_policy_document" "queue" {
  statement {
    effect = "Allow"

    principals {
      type = "*"
      identifiers = ["*"]
    }

    actions = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.this.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values = [aws_s3_bucket.this.arn]
    }
  }
}

resource "aws_s3_bucket_notification" "this" {
  bucket = aws_s3_bucket.this.id

  queue {
    queue_arn     = aws_sqs_queue.this.arn
    events = ["s3:ObjectCreated:*"]
    filter_suffix = ".parquet"
  }
}

output "s3_bucket_name" {
  value = aws_s3_bucket.this.id
}

output "sqs_queue_url" {
  value = aws_sqs_queue.this.id
}
