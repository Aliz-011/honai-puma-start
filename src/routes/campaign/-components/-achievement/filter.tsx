import { useQuery } from "@tanstack/react-query"
import { useMemo, useState } from "react"
import { Funnel, Loader2, Search } from "lucide-react"
import { useQueryStates, parseAsString } from 'nuqs'

import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select"
import {
    Field,
} from "@/components/ui/field"
import { getBranches, getClusters, getKabupatens } from "@/data/territories"
import { RiDownload2Line } from "@remixicon/react"
import { toast } from "sonner"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"

const searchParams = {
    branch: parseAsString.withDefault(''),
    cluster: parseAsString.withDefault(''),
    kabupaten: parseAsString.withDefault('')
}

export const Filter = () => {
    // URL state (what triggers the actual query)
    const [urlFilters, setUrlFilters] = useQueryStates(searchParams)

    const [localBranch, setLocalBranch] = useState(urlFilters.branch)
    const [localCluster, setLocalCluster] = useState(urlFilters.cluster)
    const [localKabupaten, setLocalKabupaten] = useState(urlFilters.kabupaten)

    const [isDownloading, setIsDownloading] = useState(false)

    const { data: branches = [] } = useQuery({
        queryKey: ['branches'],
        queryFn: async () => await getBranches(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
    })

    const { data: clusters = [] } = useQuery({
        queryKey: ['clusters'],
        queryFn: async () => await getClusters(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
    })

    const { data: kabupatens = [] } = useQuery({
        queryKey: ['kabupatens'],
        queryFn: async () => await getKabupatens(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
    })

    const filteredClusters = useMemo(() => {
        if (!localBranch) {
            const validBranchIds = new Set(branches.map(b => b.territory))
            return clusters.filter(cluster => validBranchIds.has(cluster.branch))
        }
        return clusters.filter(cluster => cluster.branch === localBranch)
    }, [branches, clusters, localBranch])

    const filteredKabupatens = useMemo(() => {
        if (!localCluster) {
            const validClusterIds = new Set(clusters.map(b => b.territory))
            return kabupatens.filter(kabupaten => validClusterIds.has(kabupaten.cluster))
        }
        return kabupatens.filter(kabupaten => kabupaten.cluster === localCluster)
    }, [clusters, kabupatens, localCluster])


    const handleApplyFilters = () => {
        setUrlFilters({
            branch: localBranch,
            cluster: localCluster,
            kabupaten: localKabupaten
        })
    }

    const handleClearFilter = () => {
        setLocalBranch('')
        setLocalCluster('')
        setLocalKabupaten('')
        setUrlFilters({
            branch: '',
            cluster: '',
            kabupaten: ''
        })
    }

    const handleDownload = async () => {
        // Sync URL filters with local state before download
        setUrlFilters({
            branch: localBranch,
            cluster: localCluster,
            kabupaten: localKabupaten
        })

        setIsDownloading(true)

        try {
            const params = new URLSearchParams({
                ...(localBranch && { branch: localBranch }),
                ...(localCluster && { cluster: localCluster }),
                ...(localKabupaten && { kabupaten: localKabupaten })
            })

            const url = `/api/campaign/wl-campaign/download?${params.toString()}`

            const response = await fetch(url)

            if (!response.ok) {
                throw new Error(`Download failed: ${response.statusText}`)
            }

            const blob = await response.blob()

            // Extract filename from Content-Disposition header
            const contentDisposition = response.headers.get('Content-Disposition')
            let filename = 'download.zip'
            if (contentDisposition) {
                const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/)
                if (filenameMatch && filenameMatch[1]) {
                    filename = filenameMatch[1].replace(/['"]/g, '')
                }
            }

            // Trigger download
            const downloadUrl = window.URL.createObjectURL(blob)
            const link = document.createElement('a')
            link.href = downloadUrl
            link.download = filename
            document.body.appendChild(link)
            link.click()
            document.body.removeChild(link)
            window.URL.revokeObjectURL(downloadUrl)

            toast.success('Downloading File')
        } catch (error) {
            console.error('Download error:', error)
            toast.error('Download failed. Please try again.')
        } finally {
            setIsDownloading(false)
        }
    }

    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-4 gap-4">
            <div className="w-full max-w-md">
                <Field>
                    <Select value={localBranch} onValueChange={setLocalBranch}>
                        <SelectTrigger>
                            <SelectValue placeholder="Branch" />
                        </SelectTrigger>
                        <SelectContent>
                            {branches.map((branch) => (
                                <SelectItem key={branch.territory!} value={branch.territory!}>
                                    {branch.territory}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>

            <div className="w-full max-w-md">
                <Field>
                    <Select value={localCluster} onValueChange={setLocalCluster}>
                        <SelectTrigger>
                            <SelectValue placeholder="Cluster" />
                        </SelectTrigger>
                        <SelectContent>
                            {filteredClusters.map((cluster) => (
                                <SelectItem key={cluster.territory} value={cluster.territory!}>
                                    {cluster.territory}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>

            <div className="w-full max-w-md">
                <Field>
                    <Select value={localKabupaten} onValueChange={setLocalKabupaten}>
                        <SelectTrigger>
                            <SelectValue placeholder="Kabupaten" />
                        </SelectTrigger>
                        <SelectContent>
                            {filteredKabupatens.map((kabupaten) => (
                                <SelectItem key={kabupaten.territory} value={kabupaten.territory!}>
                                    {kabupaten.territory}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>

            <div className="flex gap-2 mt-auto">
                <Button
                    onClick={handleDownload}
                    disabled={isDownloading}
                    className={cn('px-4 py-2 text-white text-xs font-medium rounded-lg transition-all duration-200 shadow-lg hover:shadow-xl', isDownloading
                        ? 'bg-gray-500 cursor-not-allowed opacity-50'
                        : 'bg-linear-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700 cursor-pointer'
                    )}
                >
                    {isDownloading ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                        <RiDownload2Line className="w-4 h-4" />
                    )}
                    {isDownloading ? 'Downloading...' : 'Download'}
                </Button>
                <Button
                    onClick={handleClearFilter}
                    className="cursor-pointer px-4 py-2 bg-linear-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white text-xs font-medium rounded-lg transition-all duration-200 shadow-lg hover:shadow-xl"
                >
                    <Funnel className="w-4 h-4" />
                    Clear
                </Button>
            </div>
        </div>
    )
}