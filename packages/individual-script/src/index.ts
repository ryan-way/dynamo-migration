import {
  AttributeAction,
  DeleteItemCommand,
  DeleteItemCommandInput,
  DynamoDBClient,
  PutItemCommand,
  ScanCommand,
  ScanCommandInput,
  UpdateItemCommand,
  UpdateItemCommandInput,
} from "@aws-sdk/client-dynamodb";
import { Profiler } from "lib";

const TABLE_ARN =
  "arn:aws:dynamodb:us-west-1:196728492750:table/DynamoStack-TestTable5769773A-WBGE6PUF9M82";
const TEST_SIZE = 100;
const SCAN_SIZE = 1;

async function insertItems(client: DynamoDBClient) {
  console.log("Inserting items...");
  for (let i = 0; i < TEST_SIZE; i++) {
    const input = {
      TableName: TABLE_ARN,
      Item: {
        pk: {
          S: `primary_key_${i}`,
        },
        num: {
          N: `${i}`,
        },
      },
    };

    const command = new PutItemCommand(input);
    await profiler.profileAsync(
      "insert",
      async () => await client.send(command),
    );
  }
}

async function updateItems(client: DynamoDBClient) {
  console.log("Updating items...");
  const scanInput: ScanCommandInput = {
    TableName: TABLE_ARN,
    Limit: SCAN_SIZE,
    ExclusiveStartKey: undefined,
  };

  do {
    const scanCommand = new ScanCommand(scanInput);
    const scanOutput = await profiler.profileAsync(
      "scan",
      async () => await client.send(scanCommand),
    );

    scanOutput.Items = scanOutput.Items ?? [];
    for (const item of scanOutput.Items) {
      const updateInput: UpdateItemCommandInput = {
        TableName: TABLE_ARN,
        Key: {
          pk: item.pk,
        },
        AttributeUpdates: {
          str: {
            Value: item.pk,
            Action: AttributeAction.PUT,
          },
        },
      };

      const updateCommand = new UpdateItemCommand(updateInput);
      await profiler.profileAsync(
        "update",
        async () => await client.send(updateCommand),
      );
    }

    scanInput.ExclusiveStartKey = scanOutput.LastEvaluatedKey;
  } while (scanInput.ExclusiveStartKey);
}

async function deleteItems(client: DynamoDBClient) {
  console.log("Deleting items...");
  const scanInput: ScanCommandInput = {
    TableName: TABLE_ARN,
    Limit: SCAN_SIZE,
    ExclusiveStartKey: undefined,
  };

  do {
    const scanCommand = new ScanCommand(scanInput);
    const scanOutput = await profiler.profileAsync(
      "scan",
      async () => await client.send(scanCommand),
    );

    scanOutput.Items = scanOutput.Items ?? [];
    for (const item of scanOutput.Items) {
      const deleteInput: DeleteItemCommandInput = {
        TableName: TABLE_ARN,
        Key: {
          pk: item.pk,
        },
      };

      const deleteCommand = new DeleteItemCommand(deleteInput);
      await profiler.profileAsync(
        "delete",
        async () => await client.send(deleteCommand),
      );
    }

    scanInput.ExclusiveStartKey = scanOutput.LastEvaluatedKey;
  } while (scanInput.ExclusiveStartKey);
}

const client = new DynamoDBClient({ profile: "dynamo-migration" });
const profiler = new Profiler();

async function main() {
  if (process.argv.length < 3) {
    console.log("Usage: npm run start [insert|update|delete]");
  }

  switch (process.argv[2]) {
    case "insert":
      await insertItems(client);
      break;
    case "update":
      await updateItems(client);
      break;
    case "delete":
      await deleteItems(client);
      break;
    default:
      throw new Error(`Unsupported command ${process.argv[2]}`);
  }

  profiler.report((time) => {
    return { "time per item": Math.round((time / TEST_SIZE) * 100) / 100 };
  });
  console.log("Done!");
}
main();
