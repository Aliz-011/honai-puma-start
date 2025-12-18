import { Hono } from "hono";
import { z } from "zod";
import { zValidator } from "@hono/zod-validator";
import { desc, count } from "drizzle-orm";
import { db } from "@/db";
import { kvWording } from "@/db/schema/auth";
import { writeFile, mkdir } from "fs/promises";
import { join } from "path";

const app = new Hono()
    .get("/", zValidator("query", z.object({
        page: z.string().optional().default("1"),
        limit: z.string().optional().default("12")
    })), async (c) => {
        const { page: pageStr, limit: limitStr } = c.req.valid("query");
        const page = parseInt(pageStr);
        const limit = parseInt(limitStr);
        const offset = (page - 1) * limit;

        const data = await db.query.kvWording.findMany({
            orderBy: [desc(kvWording.createdAt)],
            limit: limit,
            offset: offset,
        });

        // Get total count for pagination
        const [totalResult] = await db.select({ count: count() }).from(kvWording);
        const total = totalResult?.count ?? 0;
        const totalPages = Math.ceil(total / limit);

        return c.json({
            data,
            pagination: {
                page,
                limit,
                total,
                totalPages
            }
        });
    })
    .post("/", zValidator("json", z.object({
        title: z.string(),
        imagePath: z.string(),
        waWording: z.string(),
        smsWording: z.string(),
    })), async (c) => {
        const body = c.req.valid("json");
        const [result] = await db.insert(kvWording).values({
            title: body.title,
            imagePath: body.imagePath,
            waWording: body.waWording,
            smsWording: body.smsWording,
        }).$returningId();

        return c.json({ success: true, id: result.id });
    })
    .post("/upload", async (c) => {
        const body = await c.req.parseBody();
        const file = body["file"];

        if (file instanceof File) {
            const buffer = await file.arrayBuffer();
            const fileName = `${Date.now()}-${file.name}`;
            const uploadDir = join(process.cwd(), "public", "uploads");

            await mkdir(uploadDir, { recursive: true });
            await writeFile(join(uploadDir, fileName), Buffer.from(buffer));

            return c.json({
                success: true,
                path: `/uploads/${fileName}`
            });
        }

        return c.json({ success: false, error: "No file uploaded" }, 400);
    });

export default app;
