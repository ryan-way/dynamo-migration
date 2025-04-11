import * as cdk from 'aws-cdk-lib';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';

export class DynamoStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    new dynamodb.TableV2(this, 'TestTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      // applies to all replicas, i.e., us-west-2, us-east-1, us-east-2
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
  }
}
