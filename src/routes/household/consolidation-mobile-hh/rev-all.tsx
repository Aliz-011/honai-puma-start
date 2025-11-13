import { createFileRoute, useSearch } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { z } from 'zod'

import { consolidationAllQueryOptions } from '@/lib/query'
import { Filters } from './-components/filters'
import { LoadingState, ErrorState, EmptyState, SuccessState } from './-components/ui-states'

export const Route = createFileRoute('/household/consolidation-mobile-hh/rev-all')({
  component: RouteComponent,
  validateSearch: z.object({
    date: z.string().optional(),
    branch: z.string().optional(),
    cluster: z.string().optional(),
    kabupaten: z.string().optional(),
  }),
  loaderDeps: ({ search }) => search,
  loader: async ({ context, deps }) => {
    await context.queryClient.prefetchQuery(consolidationAllQueryOptions(deps))
    return {}
  },
})

function RouteComponent() {
  const search = useSearch({ from: Route.id })
  const { data, isPending, error, isError } = useQuery(
    consolidationAllQueryOptions(search)
  )

  const hasData = !!data && data.length > 0

  return (
    <div className="min-h-screen space-y-6 overflow-hidden rounded-2xl border border-gray-200 bg-gray-50 px-5 py-7 dark:border-gray-800 dark:bg-white/3">
      {/* Filters */}
      <Filters daysBehind={2} isLoading={isPending} />

      <div className="min-h-[400px]">
        {isPending ? (
          <LoadingState />
        ) : isError ? (
          <ErrorState error={error} hasData={hasData} />
        ) : !hasData ? (
          <EmptyState />
        ) : (
          <SuccessState data={data} title="REV ALL MOBILE vs REV HH ALL" isLoading={isPending} />
        )}
      </div>
    </div>
  )
}