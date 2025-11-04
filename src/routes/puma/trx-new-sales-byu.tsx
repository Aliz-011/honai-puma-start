import { createFileRoute } from '@tanstack/react-router'
import { parseAsString, useQueryStates } from 'nuqs'
import { format, subDays } from 'date-fns'
import { useQuery } from '@tanstack/react-query'

import { Filters } from './-components/filters'
import { client } from '@/lib/client'
import { DataTable } from './-components/data-table'

const searchParams = {
    date: parseAsString.withDefault(format(subDays(new Date(), 2), 'yyyy-MM-dd')),
    branch: parseAsString.withDefault(''),
    subbranch: parseAsString.withDefault(''),
    cluster: parseAsString.withDefault(''),
    kabupaten: parseAsString.withDefault(''),
}

export const Route = createFileRoute('/puma/trx-new-sales-byu')({
    component: RouteComponent,
})

function RouteComponent() {
    const [{ date, branch, subbranch, cluster, kabupaten }] = useQueryStates(searchParams)

    const { data, refetch, isFetching, error, isError } = useQuery({
        queryKey: ['trx-new-sales-byu', date, branch, subbranch, cluster, kabupaten],
        queryFn: async ({ queryKey }) => {
            const [_key, dateKey, branchKey, subbranchKey, clusterKey, kabupatenKey] = queryKey

            const response = await client.api['trx-new-sales-byu'].$get({
                query: {
                    date: dateKey as string,
                    branch: branchKey as string,
                    subbranch: subbranchKey as string,
                    cluster: clusterKey as string,
                    kabupaten: kabupatenKey as string
                }
            })

            return await response.json()
        },
        staleTime: 1000 * 60,
        refetchOnWindowFocus: false
    })

    if (isError) {
        return (
            <div className="px-4 lg:px-6">
                <div className="overflow-hidden min-h-screen rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/3 space-y-4">
                    <Filters daysBehind={2} />
                    <div className="p-4 bg-red-50 border border-red-200 rounded text-red-700">
                        Failed to load data: {error?.message || 'Unknown error'}
                        <button onClick={() => refetch()} className="ml-2 underline">Retry</button>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className="px-4 lg:px-6">
            <div className="overflow-hidden min-h-screen rounded-2xl border border-gray-200 bg-white px-5 py-7 dark:border-gray-800 dark:bg-white/3 space-y-4">
                <Filters daysBehind={2} />
                <DataTable data={data} refetch={refetch} latestUpdatedData={2} title="Revenue New Sales Byu" date={date} isLoading={isFetching} />
            </div>
        </div>
    )
}
