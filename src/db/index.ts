import { config } from 'dotenv'

import { drizzle } from 'drizzle-orm/mysql2'
import mysql from 'mysql2/promise'

import * as schema from './todos'

config()

const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: 'puma_2025'
})
export const db = drizzle({ client: connection, mode: 'default', schema })
