import { useQuery } from '@tanstack/react-query'
import { useState } from 'react'
import { createFileRoute } from '@tanstack/react-router'
import {
  createStandardSchemaV1,
  parseAsString,
  useQueryStates
} from 'nuqs'

import { Filters } from './-components/-revenue-c3mr/filter'
import { client } from '@/lib/client'
import { ProgressCard } from './-components/-revenue-c3mr/progress-card'
import { Button } from '@/components/ui/button'

const searchParams = {
  branch: parseAsString.withDefault(''),
  wok: parseAsString.withDefault(''),
  date: parseAsString.withDefault('')
}

export const Route = createFileRoute('/household/revenue-c3mr')({
  component: RouteComponent,
  validateSearch: createStandardSchemaV1(searchParams, {
    partialOutput: true
  })
})

function RouteComponent() {
  const [{ branch, date, wok }] = useQueryStates(searchParams)
  const [fetchDataClicked, setFetchDataClicked] = useState(false);

  const { data, isLoading, error, isRefetching, isError, isSuccess } = useQuery({
    queryKey: ['revenue-c3mr', date, branch, wok],
    queryFn: async ({ queryKey }) => {
      const [_key, dateQuery, branchQuery, wokQuery] = queryKey;

      let dateString: string | undefined;
      if (dateQuery) {
        const d = new Date(dateQuery);
        if (isNaN(d.getTime()) || !d.toISOString().startsWith(dateQuery)) {
          throw new Error(`Invalid date format: ${dateQuery}`);
        }
        dateString = dateQuery;
      }

      const response = await client.api.household['revenue-c3mr'].$get({
        query: {
          date: dateString,
          branch: branchQuery as string,
          wok: wokQuery as string
        }
      })

      if (!response.ok) {
        throw new Error('Failed to fetch data')
      }
      const { data } = await response.json()
      return data
    },
    refetchOnWindowFocus: false,
    retry: 1,
    gcTime: 5 * 60 * 1000,
  })

  const isDataActuallyAvailable = data && data.length > 0;

  return (
    <div className="overflow-hidden rounded-xl space-y-4">
      <div className="py-2">
        <Filters daysBehind={2} disabled={isLoading || isRefetching} />
      </div>

      {(() => {
        if (isLoading || isRefetching) {
          return (
            <div className="flex h-full items-center justify-center">
              <p>Loading data...</p>
            </div>
          );
        }

        if (isError) {
          if (!isDataActuallyAvailable) {
            return (
              <div className="flex h-full items-center justify-center">
                <p>Error: {error?.message}</p>
              </div>
            );
          }
        }

        if (isDataActuallyAvailable) {
          return (
            <div className="space-y-12">
              {isError && (
                <div className="flex h-full items-center justify-center text-red-500">
                  <p>Warning: Failed to update data. Displaying last available data. Error: {error?.message}</p>
                </div>
              )}

              <section className="space-y-6">
                <div>
                  <h2 className="text-2xl font-bold text-gray-900 dark:text-white">Revenue Billing Ach</h2>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-4 gap-8">
                  <ProgressCard
                    title='Revenue - All'
                    subtitle="Rev"
                    data={[
                      { label: 'Target', value: data[0].target_rev_all },
                      { label: 'Paid', value: data[0].bill_amount_all },
                      { label: 'Unpaid', value: data[0].bill_amount_all_unpaid },
                      { label: 'Ach', value: data[0].ach_fm_rev_all, style: getValueStyle(parseFloat(data[0].ach_fm_rev_all.replace('%', '')), true) },
                      { label: 'Gap', value: data[0].gap_to_target_rev_all, style: getValueStyle(data[0].gap_to_target_rev_all, false, true) },
                    ]}
                  />

                  <ProgressCard
                    title='Revenue - Existing'
                    subtitle="Rev"
                    data={[
                      { label: 'Target', value: data[0].target_rev_existing },
                      { label: 'Paid', value: data[0].bill_amount_existing },
                      { label: 'Unpaid', value: data[0].bill_amount_existing_unpaid },
                      { label: 'Ach', value: data[0].ach_fm_rev_existing, style: getValueStyle(parseFloat(data[0].ach_fm_rev_existing.replace('%', '')), true) },
                      { label: 'Gap', value: data[0].gap_to_target_rev_existing, style: getValueStyle(data[0].gap_to_target_rev_existing, false, true) },
                    ]}
                  />

                  <ProgressCard
                    title='Revenue - New Sales'
                    subtitle="Rev"
                    data={[
                      { label: 'Target', value: data[0].target_rev_ns },
                      { label: 'Paid', value: data[0].bill_amount_ns },
                      { label: 'Unpaid', value: data[0].bill_amount_ns_unpaid },
                      { label: 'Ach', value: data[0].ach_fm_rev_ns, style: getValueStyle(parseFloat(data[0].ach_fm_rev_ns.replace('%', '')), true) },
                      { label: 'Gap', value: data[0].gap_to_target_rev_ns, style: getValueStyle(data[0].gap_to_target_rev_ns, false, true) },
                    ]}
                  />

                  <ProgressCard
                    title='Loss Revenue'
                    subtitle="Rev"
                    data={[
                      { label: 'Rev NS', value: data[0].bill_amount_ns },
                      { label: 'Rev All Unpaid', value: data[0].bill_amount_all_unpaid },
                      { label: 'Total', value: (data[0].bill_amount_ns - data[0].bill_amount_all_unpaid).toFixed(2) },
                    ]}
                  />
                </div>
              </section>

              <section className="space-y-6">
                <div>
                  <h2 className="text-2xl font-bold text-gray-900 dark:text-white">Ratio C3MR by LoS</h2>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                  <ProgressCard
                    title='LoS 0-6'
                    subtitle="Sub"
                    data={[
                      { label: 'Subs', value: data[0].subs_0_6.toLocaleString('id-ID') },
                      { label: 'Paid', value: data[0].subs_paid_0_6.toLocaleString('id-ID') },
                      { label: 'Unpaid', value: (data[0].subs_0_6 - data[0].subs_paid_0_6).toLocaleString('id-ID') },
                      { label: 'Ach', value: data[0].ach_subs_0_6, style: getValueStyle(parseFloat(data[0].ach_subs_0_6.replace('%', '')), true) },
                    ]}
                  />

                  <ProgressCard
                    title='LoS >6'
                    subtitle="Sub"
                    data={[
                      { label: 'Subs', value: data[0].subs_gt_6.toLocaleString('id-ID') },
                      { label: 'Paid', value: data[0].subs_paid_gt_6.toLocaleString('id-ID') },
                      { label: 'Unpaid', value: (data[0].subs_gt_6 - data[0].subs_paid_gt_6).toLocaleString('id-ID') },
                      { label: 'Ach', value: data[0].ach_subs_paid_gt_6, style: getValueStyle(parseFloat(data[0].ach_subs_paid_gt_6.replace('%', '')), true) },
                    ]}
                  />
                </div>
              </section>
            </div>
          );
        }

        if (fetchDataClicked && (isSuccess || isError /* an attempt was made */) && !isDataActuallyAvailable) {
          return (
            <div className="flex h-full items-center justify-center">
              <p>No data found for the selected filters.</p>
            </div>
          );
        }

        // Initial state: no fetch attempted yet
        if (!fetchDataClicked) {
          return (
            <div className="flex h-full items-center justify-center">
              <Button
                onClick={() => setFetchDataClicked(true)}
                variant='outline'
              >
                Tampilkan data
              </Button>
            </div>
          );
        }

        return null;
      })()}
    </div>
  )
}

const getValueStyle = (value: number, isPercentage = false, isGap = false) => {
  if (isGap) {
    return value > 0 ? 'bg-gradient-to-br from-emerald-500 to-emerald-600 text-white' :
      value < 0 ? 'bg-gradient-to-br from-rose-500 to-rose-600 text-white' :
        'bg-gradient-to-br from-gray-500 to-gray-600 text-white';
  }

  if (isPercentage) {
    return value >= 100 ? 'bg-gradient-to-br from-emerald-500 to-emerald-600 text-white' : 'bg-gradient-to-br from-rose-500 to-rose-600 text-white';
  }

  return 'bg-gradient-to-br from-gray-500 to-gray-600 text-white';
};