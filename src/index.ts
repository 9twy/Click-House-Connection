import express, { Request, Response } from "express";
import { ClickHouseClient, createClient } from "@clickhouse/client";
import { v4 as uuidv4 } from "uuid";
import { faker } from "@faker-js/faker";
import { rateLimit } from "express-rate-limit";
import RedisStore from "rate-limit-redis";
import { createClient as createRedisClient } from "redis";

const app = express();

app.use(express.json());

const client: ClickHouseClient = createClient({
  host: "http://localhost:8123",
  username: "default",
  password: "",
});

const redisClient = createRedisClient({
  url: "redis://localhost:6379",
});

const createTableQuery = `
  CREATE TABLE IF NOT EXISTS users (
    id UUID,
    name String,
    age UInt8,
    created_at DateTime
  ) ENGINE = MergeTree()
  ORDER BY id;
`;

async function initDatabase() {
  try {
    await client.exec({ query: createTableQuery });
    console.log("Table 'users' created or already exists.");
  } catch (error) {
    console.error("Error", error);
  }
}
redisClient.connect().catch(console.error);
const limiter = rateLimit({
  store: new RedisStore({
    sendCommand: (...args: string[]) => redisClient.sendCommand(args),
  }),
  windowMs: 15 * 60 * 1000, // 15 minutes
  limit: 5, // Limit each IP to 100 requests per `window` (here, per 15 minutes)
  message: "Too many requests from this IP.",
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
});
app.post("/users", async (req: Request, res: Response) => {
  const { name, age } = req.body;
  console.log(name);

  const insertUserQuery = `
    INSERT INTO users (id, name, age, created_at) VALUES 
    ('${uuidv4()}', '${name}', ${age}, now());
  `;
  console.log(name);

  try {
    await client.exec({ query: insertUserQuery });
    res.status(201).json({ message: "User created successfully" });
  } catch (error) {
    console.error("Error inserting user:", error);
    res.status(500).json({ message: "Error creating user" });
  }
});

// here is inserting a 10K users in one time.
// the data genrated by faker.

app.post("/users1", async (req: Request, res: Response) => {
  try {
    const batchSize = 10000;
    const values = [];

    for (let i = 0; i < batchSize; i++) {
      const name = faker.person.firstName().replace(/'/g, "''");
      const age = faker.number.int({ min: 1, max: 20 });
      console.log(name, age);

      values.push(`('${uuidv4()}', '${name}', ${age}, now())`);
    }

    const insertUserQuery = `
      INSERT INTO users (id, name, age, created_at) VALUES ${values.join(",")}
    `;
    await client.exec({ query: insertUserQuery });
    res
      .status(201)
      .json({ message: "inserted successfully by batching 10K !" });
  } catch (error) {
    console.error("Error :", error);
    res.status(500).json({ message: "Error " });
  }
});

// this approach is too slow .. for inserting a 1000 recored its take more than 15 min.

// app.post("/users1", async (req: Request, res: Response) => {
//   const name = "test";
//   const age = 34;

//   try {
//     for (let index = 0; index < 1000; index++) {
//       const insertUserQuery = `
//     INSERT INTO users (id, name, age, created_at) VALUES
//     ('${uuidv4()}', '${name + index}', ${age}, now());
//   `;

//       await client.exec({ query: insertUserQuery });
//       console.log("row: " + index);
//     }

//     res.status(201).json({ message: "User created successfully" });
//   } catch (error) {
//     console.error("Error inserting user:", error);
//     res.status(500).json({ message: "Error creating user" });
//   }
// });

app.use("/users", limiter);
// get 100 user from the table
app.get("/users", async (req: Request, res: Response) => {
  try {
    const resultSet = await client.query({
      query: "SELECT * FROM users ORDER BY created_at DESC LIMIT 100",
      format: "JSONEachRow",
    });
    const users = await resultSet.json();
    res.json(users);
  } catch (error) {
    console.error("Error fetching users:", error);
    res.status(500).json({ message: "Error fetching users" });
  }
});
const port = 8011;
initDatabase().then(() => {
  app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
  });
});

app.use("/users/count", limiter);
// this counts how many users is in the table.
app.get("/users/count", async (req: Request, res: Response) => {
  try {
    const resultSet = await client.query({
      query: "SELECT count() AS c FROM users",
      format: "JSONEachRow",
    });
    const result = await resultSet.json();
    const typedResult = result as { c: string }[];

    console.log("ClickHouse Response:", result);
    console.log("Type of result:", typeof result);

    if (typedResult && typedResult.length > 0) {
      const userCountString = typedResult[0].c;
      const userCount = parseInt(userCountString, 10);
      res.json({ userCount });
    } else {
      res.status(404).json({ message: "No data found" });
    }
  } catch (error) {
    console.error("Error fetching user count:", error);
    res.status(500).json({ message: "Error fetching user count" });
  }
});
