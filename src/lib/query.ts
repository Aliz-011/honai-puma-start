import { getIsomorphicClient } from '@/lib/client'
import { queryOptions } from '@tanstack/react-query'

type SearchConsolidate = {
    date?: string
    branch?: string
    cluster?: string
    kabupaten?: string
}

/** Helper that works both on server & client */
export const fetchConsolidationAll = async (search: SearchConsolidate) => {
    const rpc = getIsomorphicClient()
    const res = await rpc.household['consolidation-all'].$get({ query: search })
    if (!res.ok) throw new Error('Failed to fetch consolidation data')
    return res.json()
}

/** Re-usable query options (type-safe) */
export const consolidationAllQueryOptions = (search: SearchConsolidate) => queryOptions({
    queryKey: ['consolidation-all', search] as const,
    queryFn: () => fetchConsolidationAll(search),
    staleTime: 5 * 60 * 1000, // 5 min
    refetchOnWindowFocus: false
})

export const fetchConsolidationBB = async (search: SearchConsolidate) => {
    const rpc = getIsomorphicClient()
    const res = await rpc.household['consolidation-bb'].$get({ query: search })
    if (!res.ok) throw new Error('Failed to fetch consolidation data')
    return res.json()
}

export const consolidationBBQueryOptions = (search: SearchConsolidate) => queryOptions({
    queryKey: ['consolidation-bb', search] as const,
    queryFn: () => fetchConsolidationBB(search),
    staleTime: 5 * 60 * 1000, // 5 min
    refetchOnWindowFocus: false
})

export const fetchConsolidationPayload = async (search: SearchConsolidate) => {
    const rpc = getIsomorphicClient()
    const res = await rpc.household['consolidation-payload'].$get({ query: search })
    if (!res.ok) throw new Error('Failed to fetch consolidation data')
    return res.json()
}

export const consolidationPayloadQueryOptions = (search: SearchConsolidate) => ({
    queryKey: ['consolidation-payload', search] as const,
    queryFn: () => fetchConsolidationPayload(search),
    staleTime: 5 * 60 * 1000, // 5 min
    refetchOnWindowFocus: false
})

type SearchSalesFulfilment = {
    date?: string
    branch?: string
    wok?: string
}

// ———————————————————————— QUERY OPTIONS ————————————————————————
export const ioRepsQueryOptions = (search: SearchSalesFulfilment) => queryOptions({
    queryKey: ['sales-fulfilment', 'io-re-ps', search] as const,
    queryFn: () => fetchIoReps(search),
    staleTime: 5 * 60 * 1000,
    retry: 1,
    refetchOnWindowFocus: false
})

export const salesForceQueryOptions = (search: SearchSalesFulfilment) => queryOptions({
    queryKey: ['sales-fulfilment', 'sf-class', search] as const,
    queryFn: () => fetchSalesForce(search),
    staleTime: 5 * 60 * 1000,
    retry: 1,
    refetchOnWindowFocus: false
})

// ———————————————————————— FETCHERS ————————————————————————
const fetchIoReps = async (search: SearchSalesFulfilment) => {
    const res = await getIsomorphicClient().household['io-re-ps'].$get({
        query: { date: search.date || undefined, branch: search.branch, wok: search.wok },
    })
    if (!res.ok) throw new Error('Failed to fetch IO/RE/PS data')
    const json = await res.json()
    return json.data
}

const fetchSalesForce = async (search: SearchSalesFulfilment) => {
    const res = await getIsomorphicClient().household['sf-class'].$get({
        query: { date: search.date || undefined, branch: search.branch, wok: search.wok },
    })
    if (!res.ok) throw new Error('Failed to fetch Sales Force data')
    const json = await res.json()
    return json.data
}