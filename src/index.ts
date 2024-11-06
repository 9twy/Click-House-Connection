import express, { Request, Response } from "express";
import { ClickHouseClient, createClient } from "@clickhouse/client";
import { v4 as uuidv4 } from "uuid"; //create a random number used for the id in the click

const app = express();

app.use(express.json());

const client: ClickHouseClient = createClient({
  host: "http://localhost:8123",
  username: "default",
  password: "",
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

app.get("/users", async (req: Request, res: Response) => {
  try {
    const resultSet = await client.query({
      query: "SELECT * FROM users ORDER BY created_at DESC LIMIT 10",
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
