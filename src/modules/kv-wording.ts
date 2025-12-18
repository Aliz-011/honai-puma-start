import { Hono } from "hono";
import { z } from "zod";
import { zValidator } from "@hono/zod-validator";
import { desc, count } from "drizzle-orm";
import { db } from "@/db";
import { kvWording } from "@/db/schema/auth";
import { writeFile, mkdir, readFile } from "fs/promises";
import { extname, join } from "path";

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
    .get("/uploads/:filename", async (c) => {
        const filename = c.req.param("filename");

        // Security: prevent traversal attacks
        if (!filename || filename.includes("..") || filename.includes("/")) {
            return c.text("Invalid filename", 400);
        }

        const isProduction = process.env.NODE_ENV === 'production';
        const uploadDir = isProduction
            ? join(process.cwd(), ".output", "public", "uploads")
            : join(process.cwd(), "public", "uploads");

        const filePath = join(uploadDir, filename);

        try {
            const file = await readFile(filePath); // or use fs/promises readFile

            const ext = extname(filename).toLowerCase();
            const mimeTypes: Record<string, string> = {
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".png": "image/png",
                ".gif": "image/gif",
                ".webp": "image/webp",
                ".svg": "image/svg+xml",
            };
            const contentType = mimeTypes[ext] || "application/octet-stream";

            return c.newResponse(file, 200, {
                "Content-Type": contentType,
                "Cache-Control": "public, max-age=604800",
            });
        } catch {
            return c.text("File not found", 404);
        }
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

        if (!(file instanceof File)) {
            return c.json({ success: false, error: "No file uploaded" }, 400);
        }

        const buffer = await file.arrayBuffer();
        const fileName = `${Date.now()}-${file.name.replace(/\s/g, '-')}`;
        // const fileName = `${Date.now()}-${file.name}`;

        // Determine environment and set appropriate upload directory
        const isProduction = process.env.NODE_ENV === 'production';

        // In production, Nitro serves static files from .output/public
        // In development, Vite serves from public
        const uploadDir = isProduction
            ? join(process.cwd(), ".output", "public", "uploads")
            : join(process.cwd(), "public", "uploads");

        await mkdir(uploadDir, { recursive: true });
        await writeFile(join(uploadDir, fileName), Buffer.from(buffer));

        // Return a path that will use our new serving route (see Step 2)
        return c.json({
            success: true,
            path: fileName
        });
    });

export default app;
