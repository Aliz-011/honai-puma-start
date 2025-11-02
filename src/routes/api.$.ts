import app from '@/db/api'
import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/api/$')({
    server: {
        handlers: {
            GET: ({ request }) => app.fetch(request),
            POST: ({ request }) => app.fetch(request),
        }
    }
})
