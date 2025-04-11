import {
  BatchWriteItemCommand,
  BatchWriteItemCommandOutput,
  DynamoDBClient,
  ScanCommand,
  ScanCommandInput,
  ScanCommandOutput,
  WriteRequest,
} from "@aws-sdk/client-dynamodb";
import { Profiler } from "lib";

const TABLE_ARN =
  "arn:aws:dynamodb:us-west-1:196728492750:table/DynamoStack-TestTable5769773A-WBGE6PUF9M82";
const TEST_SIZE = 100000;
const DYNAMO_DB_MAX_BATCH_SIZE = 25;
const SCAN_SIZE = 100;
const SEGMENT_SIZE = 100;

async function insertItems(client: DynamoDBClient) {
  console.log("Inserting items...");
  const promises: Promise<BatchWriteItemCommandOutput>[] = [];
  let requests: WriteRequest[] = [];
  for (let i = 0; i < TEST_SIZE; i++) {
    requests.push({
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

    if (requests.length == DYNAMO_DB_MAX_BATCH_SIZE) {
      const command = new BatchWriteItemCommand({
        RequestItems: {
          "DynamoStack-TestTable5769773A-WBGE6PUF9M82": [...requests],
        },
      });
      promises.push(client.send(command));
      requests = [];
    }
  }

  await profiler.profileAsync(
    "concurrent batch insert",
    async () => await Promise.all(promises),
  );
}

async function updateItems(client: DynamoDBClient) {
  console.log("Updating items...");
  let count = 0;
  const countInput: ScanCommandInput = {
    TableName: TABLE_ARN,
    Limit: SCAN_SIZE,
    Select: "COUNT",
    ExclusiveStartKey: undefined,
  };

  do {
    const countCommand = new ScanCommand(countInput);
    const countOutput = await profiler.profileAsync(
      "count",
      async () => await client.send(countCommand),
    );

    count += countOutput.Count ?? 0;
    countInput.ExclusiveStartKey = countOutput.LastEvaluatedKey;
  } while (countInput.ExclusiveStartKey);

  const scanPromises: Promise<ScanCommandOutput>[] = [];
  const segmentCount = Math.ceil(count / SEGMENT_SIZE);
  for (let i = 0; i < segmentCount; i++) {
    const scanInput: ScanCommandInput = {
      TableName: TABLE_ARN,
      Limit: SCAN_SIZE,
      Segment: i,
      TotalSegments: segmentCount,
    };
    const scanCommand = new ScanCommand(scanInput);
    scanPromises.push(client.send(scanCommand));
  }

  const items = (
    await profiler.profileAsync(
      "parallel scan",
      async () => await Promise.all(scanPromises),
    )
  ).flatMap((output) => output.Items ?? []);

  const requestPromises: Promise<BatchWriteItemCommandOutput>[] = [];
  let requests: WriteRequest[] = [];
  for (const item of items) {
    requests.push({
      PutRequest: {
        Item: {
          ...item,
          str: item.pk,
        },
      },
    });

    if (requests.length == DYNAMO_DB_MAX_BATCH_SIZE) {
      const command = new BatchWriteItemCommand({
        RequestItems: {
          "DynamoStack-TestTable5769773A-WBGE6PUF9M82": [...requests],
        },
      });
      requestPromises.push(client.send(command));
      requests = [];
    }
  }

  await profiler.profileAsync(
    "concurrent batch update",
    async () => await Promise.all(requestPromises),
  );
}

async function deleteItems(client: DynamoDBClient) {
  console.log("Deleting items...");
  let count = 0;
  const countInput: ScanCommandInput = {
    TableName: TABLE_ARN,
    Limit: SCAN_SIZE,
    Select: "COUNT",
    ExclusiveStartKey: undefined,
  };

  do {
    const countCommand = new ScanCommand(countInput);
    const countOutput = await profiler.profileAsync(
      "count",
      async () => await client.send(countCommand),
    );

    count += countOutput.Count ?? 0;
    countInput.ExclusiveStartKey = countOutput.LastEvaluatedKey;
  } while (countInput.ExclusiveStartKey);

  const scanPromises: Promise<ScanCommandOutput>[] = [];
  const segmentCount = Math.ceil(count / SEGMENT_SIZE);
  for (let i = 0; i < segmentCount; i++) {
    const scanInput: ScanCommandInput = {
      TableName: TABLE_ARN,
      Limit: SCAN_SIZE,
      Segment: i,
      TotalSegments: segmentCount,
    };
    const scanCommand = new ScanCommand(scanInput);
    scanPromises.push(client.send(scanCommand));
  }

  const items = (
    await profiler.profileAsync(
      "parallel scan",
      async () => await Promise.all(scanPromises),
    )
  ).flatMap((output) => output.Items ?? []);

  const requestPromises: Promise<BatchWriteItemCommandOutput>[] = [];
  let requests: WriteRequest[] = [];
  for (const item of items) {
    requests.push({
      DeleteRequest: {
        Key: {
          pk: item.pk,
        },
      },
    });

    if (requests.length == DYNAMO_DB_MAX_BATCH_SIZE) {
      const command = new BatchWriteItemCommand({
        RequestItems: {
          "DynamoStack-TestTable5769773A-WBGE6PUF9M82": [...requests],
        },
      });
      requestPromises.push(client.send(command));
      requests = [];
    }
  }

  await profiler.profileAsync(
    "concurrent batch delete",
    async () => await Promise.all(requestPromises),
  );
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
