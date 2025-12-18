import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { toast } from 'sonner'
import { Button } from "@/components/ui/button"
import { CopyIcon, DownloadIcon, ChevronLeft, ChevronRight } from "lucide-react"
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"

export const Route = createFileRoute('/campaign/kv-wording')({
    component: KVWordingPage,
})

type KVItem = {
    id: string
    title: string
    imagePath: string
    waWording: string
    smsWording: string
    createdAt: string
}

type APIResponse = {
    data: KVItem[]
    pagination: {
        page: number
        limit: number
        total: number
        totalPages: number
    }
}

function KVWordingPage() {
    const [page, setPage] = useState(1)
    const limit = 9 // 3x3 Grid

    const { data: response, isLoading, error } = useQuery({
        queryKey: ['kv-wording', page],
        queryFn: async () => {
            const res = await fetch(`/api/campaign/kv?page=${page}&limit=${limit}`)
            if (!res.ok) throw new Error('Failed to fetch data')
            return res.json() as Promise<APIResponse>
        },
        placeholderData: (previousData) => previousData // Keep prev data while fetching new
    })

    const getImageUrl = (path: string) => {
        if (!path) return '';
        if (path.startsWith('http')) return path;

        // Clean up path if it contains "uploads/" already from legacy data
        const filename = path.replace('/uploads/', '').replace('uploads/', '');

        // Point to the Hono Route defined in kv-wording.ts
        return `/api/campaign/kv/uploads/${filename}`;
    }

    const handleCopy = (text: string, type: string) => {
        navigator.clipboard.writeText(text)
        toast.success(`${type} wording copied to clipboard`)
    }

    const handleDownload = async (item: KVItem) => {
        try {
            const imageUrl = getImageUrl(item.imagePath);
            const response = await fetch(imageUrl);
            if (!response.ok) throw new Error("Image not found");

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            // Ensure nice filename
            const cleanName = item.imagePath.replace('/uploads/', '');
            link.download = cleanName || `campaign-${item.id}.png`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(url);
        } catch (e) {
            console.error(e)
            toast.error("Failed to download image")
        }
    }

    if (isLoading && !response) return <div className="p-8 text-center">Loading...</div>
    if (error) return <div className="p-8 text-center text-red-500">Error loading data</div>

    const { data: items, pagination } = response || { data: [], pagination: { totalPages: 0 } }

    return (
        <div className="flex flex-col gap-6 p-4 mx-auto w-full">
            <div className="flex items-center justify-between">
                <h1 className="text-3xl font-bold tracking-tight">Campaign Materials</h1>
            </div>

            {items.length === 0 ? (
                <div className="text-center p-12 text-muted-foreground border rounded-lg border-dashed">
                    No campaign materials found.
                </div>
            ) : (
                <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-4 gap-6">
                    {items.map((item) => (
                        <Card key={item.id} className="overflow-hidden flex flex-col">
                            {/* Instagram-style Header */}
                            <CardHeader className="px-3 flex items-center gap-2 border-b">
                                <CardTitle className="font-semibold text-sm">{item.title}</CardTitle>
                            </CardHeader>

                            {/* Image Area */}
                            <div className="aspect-square relative bg-muted">
                                <img
                                    src={getImageUrl(item.imagePath)}
                                    alt={item.title}
                                    className="w-full h-full object-cover transition-transform duration-300 group-hover:scale-105"
                                    loading="lazy"
                                    onError={(e) => {
                                        // Fallback if image fails to load
                                        e.currentTarget.style.display = 'none';
                                        e.currentTarget.nextElementSibling?.classList.remove('hidden');
                                    }}
                                />
                            </div>

                            {/* Actions Bar */}
                            <div className="p-3 flex items-center justify-between border-b">
                                <Button variant="ghost" size="sm" onClick={() => handleDownload(item)}>
                                    <DownloadIcon className="h-5 w-5 mr-2" />
                                    Download
                                </Button>
                            </div>

                            {/* Wording Content */}
                            <CardContent className="p-4 space-y-4 flex-1">
                                <div className="space-y-2">
                                    <div className="flex items-center justify-between">
                                        <span className="text-xs font-semibold uppercase text-muted-foreground">WhatsApp</span>
                                        <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => handleCopy(item.waWording, 'WA')}>
                                            <CopyIcon className="h-3 w-3" />
                                        </Button>
                                    </div>
                                    <div className="text-sm line-clamp-3 bg-muted/50 p-2 rounded">
                                        {item.waWording}
                                    </div>
                                </div>

                                <div className="space-y-2">
                                    <div className="flex items-center justify-between">
                                        <span className="text-xs font-semibold uppercase text-muted-foreground">SMS</span>
                                        <Button variant="ghost" size="icon" className="h-6 w-6" onClick={() => handleCopy(item.smsWording, 'SMS')}>
                                            <CopyIcon className="h-3 w-3" />
                                        </Button>
                                    </div>
                                    <div className="text-sm line-clamp-3 bg-muted/50 p-2 rounded">
                                        {item.smsWording}
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    ))}
                </div>
            )}

            {/* Pagination */}
            {pagination.totalPages > 1 && (
                <div className="flex items-center justify-center gap-2 py-4">
                    <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setPage(p => Math.max(1, p - 1))}
                        disabled={page === 1}
                    >
                        <ChevronLeft className="h-4 w-4" />
                    </Button>
                    <span className="text-sm">
                        Page {page} of {pagination.totalPages}
                    </span>
                    <Button
                        variant="outline"
                        size="icon"
                        onClick={() => setPage(p => Math.min(pagination.totalPages, p + 1))}
                        disabled={page === pagination.totalPages}
                    >
                        <ChevronRight className="h-4 w-4" />
                    </Button>
                </div>
            )}
        </div>
    )
}
