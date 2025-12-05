import { formatDate, subDays } from "date-fns"
import { useQuery } from "@tanstack/react-query"
import { useMemo, useState } from "react"
import { Funnel, Loader2 } from "lucide-react"
import { useQueryStates, parseAsString } from 'nuqs'
import { RiDownload2Line } from "@remixicon/react"
import { toast } from "sonner"

import 'react-datepicker/dist/react-datepicker.css';


import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select"
import {
    Field
} from "@/components/ui/field"
import { getBranches, getClusters, getKabupatens, getKecamatans } from "@/data/territories"
import { Input } from "@/components/ui/input"

const searchParams = {
    date: parseAsString.withDefault(''),
    branch: parseAsString.withDefault(''),
    cluster: parseAsString.withDefault(''),
    kabupaten: parseAsString.withDefault(''),
    kecamatan: parseAsString.withDefault(''),
    method: parseAsString.withDefault(''),
    product_offer: parseAsString.withDefault(''),
    rows: parseAsString.withDefault('0'),
}

export const Filter = () => {
    // URL state (what triggers the actual query)
    const [urlFilters, setUrlFilters] = useQueryStates(searchParams)

    const [localDate, setLocalDate] = useState<Date | null>(
        urlFilters.date ? new Date(urlFilters.date) : null
    )
    const [localBranch, setLocalBranch] = useState(urlFilters.branch)
    const [localCluster, setLocalCluster] = useState(urlFilters.cluster)
    const [localKabupaten, setLocalKabupaten] = useState(urlFilters.kabupaten)
    const [localKecamatan, setLocalKecamatan] = useState(urlFilters.kecamatan)
    const [localMethod, setLocalMethod] = useState(urlFilters.method)
    const [localRows, setLocalRows] = useState(urlFilters.rows)
    const [localProductOffer, setLocalProductOffer] = useState(urlFilters.product_offer)

    const [isDownloading, setIsDownloading] = useState(false)

    const { data: branches = [] } = useQuery({
        queryKey: ['branches'],
        queryFn: async () => await getBranches(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const { data: clusters = [] } = useQuery({
        queryKey: ['clusters'],
        queryFn: async () => await getClusters(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const { data: kabupatens = [] } = useQuery({
        queryKey: ['kabupatens'],
        queryFn: async () => await getKabupatens(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
    })

    const { data: kecamatans = [] } = useQuery({
        queryKey: ['kecamatans'],
        queryFn: async () => await getKecamatans(),
        staleTime: 5 * 60 * 1000,
        retry: 2,
        refetchOnWindowFocus: false
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

    const filteredKecamatans = useMemo(() => {
        if (!localKabupaten) {
            const validKabupatenIds = new Set(kabupatens.map(b => b.territory))
            return kecamatans.filter(kecamatan => validKabupatenIds.has(kecamatan.kabupaten))
        }
        return kecamatans.filter(kecamatan => kecamatan.kabupaten === localKabupaten)
    }, [kabupatens, kecamatans, localKabupaten])

    const handleApplyFilters = () => {
        setUrlFilters({
            date: localDate ? formatDate(localDate, 'yyyy-MM-dd') : '',
            branch: localBranch,
            cluster: localCluster,
            kabupaten: localKabupaten,
            kecamatan: localKecamatan,
            method: localMethod,
            rows: localRows,
            product_offer: localProductOffer,
        })
    }

    const handleClearFilter = () => {
        setLocalDate(subDays(new Date(), 2))
        setLocalBranch('')
        setLocalCluster('')
        setLocalKabupaten('')
        setLocalKecamatan('')
        setLocalMethod('')
        setLocalRows('0')
        setLocalProductOffer('')
        setUrlFilters({
            date: '',
            branch: '',
            cluster: '',
            kabupaten: '',
            kecamatan: '',
            method: '',
            product_offer: '',
            rows: '0',
        })
    }

    const handleDownload = async () => {
        // Sync URL filters with local state before download
        setUrlFilters({
            date: localDate ? formatDate(localDate, 'yyyy-MM-dd') : '',
            branch: localBranch,
            cluster: localCluster,
            kabupaten: localKabupaten,
            kecamatan: localKecamatan,
            method: localMethod,
            rows: localRows,
            product_offer: localProductOffer,
        })

        setIsDownloading(true)

        try {
            const params = new URLSearchParams({
                ...(localDate && { date: formatDate(localDate, 'yyyy-MM-dd') }),
                ...(localBranch && { branch: localBranch }),
                ...(localCluster && { cluster: localCluster }),
                ...(localKabupaten && { kabupaten: localKabupaten }),
                ...(localKecamatan && { kecamatan: localKecamatan }),
                ...(localMethod && { method: localMethod }),
                ...(localProductOffer && { product_offer: localProductOffer }),
                ...(localRows && { rows: localRows }),
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
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
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
                    <Select value={localCluster} onValueChange={setLocalCluster} disabled={!localBranch}>
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
                    <Select value={localKabupaten} onValueChange={setLocalKabupaten} disabled={!localCluster}>
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

            <div className="w-full max-w-md">
                <Field>
                    <Select value={localKecamatan} onValueChange={setLocalKecamatan} disabled={!localKabupaten}>
                        <SelectTrigger>
                            <SelectValue placeholder="Kecamatan" />
                        </SelectTrigger>
                        <SelectContent>
                            {filteredKecamatans.map((kecamatan) => (
                                <SelectItem key={kecamatan.kecamatan} value={kecamatan.kecamatan!}>
                                    {kecamatan.kecamatan}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                </Field>
            </div>

            <div className="w-full max-w-md">
                <Field>
                    <Select value={localMethod} onValueChange={setLocalMethod}>
                        <SelectTrigger>
                            <SelectValue placeholder="Campaign Method" />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="sms">SMS Blast</SelectItem>
                            <SelectItem value="wa">WA Blast</SelectItem>
                        </SelectContent>
                    </Select>
                </Field>
            </div>

            <div className="w-full max-w-md">
                <Field>
                    <Select value={localProductOffer} onValueChange={setLocalProductOffer}>
                        <SelectTrigger>
                            <SelectValue placeholder="Product Offer" />
                        </SelectTrigger>
                        <SelectContent>
                            <SelectItem value="wifi_lapser">Wifi Lapser</SelectItem>
                            <SelectItem value="trade_in">Trade-in</SelectItem>
                            <SelectItem value="ss_compete">SS Compete</SelectItem>
                            <SelectItem value="pelanggan_baru">Pelanggan Baru</SelectItem>
                            <SelectItem value="lifestage_3">Lifestage 3</SelectItem>
                            <SelectItem value="reguler_package">Reguler Package</SelectItem>
                        </SelectContent>
                    </Select>
                </Field>
            </div>

            <div className="w-full max-w-md">
                <Field>
                    <Input
                        type="number"
                        placeholder="Rows Count e.g. 2000"
                        value={localRows}
                        onChange={(e) => {
                            const value = e.target.value;
                            // Allow empty string or digits only
                            if (value === '' || /^\d+$/.test(value)) {
                                setLocalRows(value);
                            }
                        }}
                        // Optional: prevent non-numeric keypresses
                        onKeyPress={(e) => {
                            if (!/[0-9]/.test(e.key)) {
                                e.preventDefault();
                            }
                        }}
                        min="0"
                        step="1"
                        className="w-full"
                    />
                </Field>
            </div>

            <div className="flex gap-2 items-start">
                <button
                    onClick={handleDownload}
                    disabled={isDownloading}
                    className={`px-4 py-2 text-white text-xs font-medium rounded-lg transition-all duration-200 shadow-lg hover:shadow-xl ${isDownloading
                        ? 'bg-gray-500 cursor-not-allowed opacity-50'
                        : 'bg-linear-to-r from-green-500 to-green-600 hover:from-green-600 hover:to-green-700 cursor-pointer'
                        }`}
                >
                    {isDownloading ? (
                        <Loader2 className="w-4 h-4 inline mr-1 animate-spin" />
                    ) : (
                        <RiDownload2Line className="w-4 h-4 inline mr-1" />
                    )}
                    {isDownloading ? 'Downloading...' : 'Download'}
                </button>
                <button
                    onClick={handleClearFilter}
                    className="cursor-pointer px-4 py-2 bg-linear-to-r from-red-500 to-red-600 hover:from-red-600 hover:to-red-700 text-white text-xs font-medium rounded-lg transition-all duration-200 shadow-lg hover:shadow-xl"
                >
                    <Funnel className="w-4 h-4 inline mr-1" />
                    Clear
                </button>
            </div>
        </div>
    )
}