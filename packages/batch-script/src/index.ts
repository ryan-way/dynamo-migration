import {
  BatchWriteItemCommand,
  BatchWriteItemCommandInput,
  DynamoDBClient,
  ScanCommand,
  ScanCommandInput,
} from "@aws-sdk/client-dynamodb";
import { Profiler } from "lib";

const TABLE_ARN =
  "arn:aws:dynamodb:us-west-1:196728492750:table/DynamoStack-TestTable5769773A-WBGE6PUF9M82";
const TEST_SIZE = 10000;
const DYNAMO_DB_MAX_BATCH_SIZE = 25;
const SCAN_SIZE = 100;

async function insertItems(client: DynamoDBClient) {
  console.log("Inserting items...");
  const input: BatchWriteItemCommandInput = {
    RequestItems: {
      "DynamoStack-TestTable5769773A-WBGE6PUF9M82": [],
    },
  };
  for (let i = 0; i < TEST_SIZE; i++) {
    input.RequestItems?.["DynamoStack-TestTable5769773A-WBGE6PUF9M82"].push({
      PutRequest: {
        Item: {
          pk: {
            S: `primary_key_${i}`,
          },
          num: {
            N: `${i}`,
          },
        },
      },
    });

    if (
      input.RequestItems?.["DynamoStack-TestTable5769773A-WBGE6PUF9M82"]
        .length == DYNAMO_DB_MAX_BATCH_SIZE
    ) {
      const command = new BatchWriteItemCommand(input);
      await profiler.profileAsync(
        "batch insert",
        async () => await client.send(command),
      );
      input.RequestItems["DynamoStack-TestTable5769773A-WBGE6PUF9M82"] = [];
    }
  }
}

async function updateItems(client: DynamoDBClient) {
  console.log("Updating items...");
  const input: BatchWriteItemCommandInput = {
    RequestItems: {
      "DynamoStack-TestTable5769773A-WBGE6PUF9M82": [],
    },
  };
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
      input.RequestItems?.["DynamoStack-TestTable5769773A-WBGE6PUF9M82"].push({
        PutRequest: {
          Item: {
            ...item,
            str: item.pk,
          },
        },
      });

      if (
        input.RequestItems?.["DynamoStack-TestTable5769773A-WBGE6PUF9M82"]
          .length == DYNAMO_DB_MAX_BATCH_SIZE
      ) {
        const command = new BatchWriteItemCommand(input);
        await profiler.profileAsync(
          "batch update",
          async () => await client.send(command),
        );
        input.RequestItems["DynamoStack-TestTable5769773A-WBGE6PUF9M82"] = [];
      }
    }

    scanInput.ExclusiveStartKey = scanOutput.LastEvaluatedKey;
  } while (scanInput.ExclusiveStartKey);
}

async function deleteItems(client: DynamoDBClient) {
  console.log("Deleting items...");
  const input: BatchWriteItemCommandInput = {
    RequestItems: {
      "DynamoStack-TestTable5769773A-WBGE6PUF9M82": [],
    },
  };
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
      input.RequestItems?.["DynamoStack-TestTable5769773A-WBGE6PUF9M82"].push({
        DeleteRequest: {
          Key: {
            pk: item.pk,
          },
        },
      });

      if (
        input.RequestItems?.["DynamoStack-TestTable5769773A-WBGE6PUF9M82"]
          .length == DYNAMO_DB_MAX_BATCH_SIZE
      ) {
        const command = new BatchWriteItemCommand(input);
        await profiler.profileAsync(
          "batch delete",
          async () => await client.send(command),
        );
        input.RequestItems["DynamoStack-TestTable5769773A-WBGE6PUF9M82"] = [];
      }
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
