import { createFileRoute } from '@tanstack/react-router'
import { useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
import { parseAsString, useQueryStates } from 'nuqs'
import { z } from 'zod'
import { Loader2 } from 'lucide-react'
import { format, parseISO, subDays } from 'date-fns'

import { SalesFulfilmentResponseData } from '@/types'

import { Filter } from './-components/-sales-fulfiment/filter'
import { SectionCards } from './-components/-sales-fulfiment/section-cards'
import { ProgressCard } from './-components/-sales-fulfiment/chart-component'
import { ChartPie } from './-components/-sales-fulfiment/chart-component'
import { FunnelingGroup, WOLoS } from './-components/-sales-fulfiment/funneling-group'
import { SalesForce } from './-components/-sales-fulfiment/sales-force'
import { FalloutDetail } from './-components/-sales-fulfiment/fallout-detail'
import { ioRepsQueryOptions, salesForceQueryOptions } from '@/lib/query'


const searchSchema = z.object({
  branch: z.string().optional(),
  wok: z.string().optional(),
  date: z.string().optional(),
})

const searchParams = {
  branch: parseAsString.withDefault(''),
  wok: parseAsString.withDefault(''),
  date: parseAsString.withDefault(''),
}

export const Route = createFileRoute('/household/sales-fulfilment')({
  validateSearch: searchSchema,
  loaderDeps: ({ search }) => search,
  loader: async ({ context, deps }) => {
    const queryClient = context.queryClient
    await Promise.all([
      queryClient.prefetchQuery(ioRepsQueryOptions(deps)),
      queryClient.prefetchQuery(salesForceQueryOptions(deps)),
    ])
    return {}
  },
  component: SalesFulfilmentPage,
  head: () => ({
    meta: [
      {
        name: 'description',
        content: 'My App is a web application',
      },
      {
        title: 'Sales Fulfilment - Household',
      },
    ],
    links: [
      {
        rel: 'icon',
        href: '/favicon.ico',
      },
    ],
  }),
})


function SalesFulfilmentPage() {
  const [{ date, branch, wok }] = useQueryStates(searchParams)

  const ioRepsQuery = useQuery(ioRepsQueryOptions({ date, branch, wok }))
  const sfQuery = useQuery(salesForceQueryOptions({ date, branch, wok }))

  const { data: ioData, isPending: ioPending, isError: ioError, error: ioErr } = ioRepsQuery
  const { data: sfData, isPending: sfPending, isError: sfError, error: sfErr } = sfQuery

  const isLoading = ioPending || sfPending
  const hasData = !!ioData?.length && !!sfData?.length

  const groupedData = useMemo(() => {
    if (!ioData) return {}
    return ioData.reduce((acc, item) => {
      if (!acc[item.level]) acc[item.level] = []
      acc[item.level].push(item)
      return acc
    }, {} as Record<string, SalesFulfilmentResponseData[]>)
  }, [ioData])

  const displayDate = format(date ? parseISO(date) : subDays(new Date(), 2), 'yyyy-MM-dd')

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="px-4 py-4">
        <Filter daysBehind={1} disabled={isLoading} />
      </div>

      <div className="mx-auto px-4 py-8 space-y-10">
        {isLoading ? (
          <LoadingState />
        ) : !hasData ? (
          <EmptyOrErrorState ioError={ioError} sfError={sfError} ioErr={ioErr} sfErr={sfErr} hasData={hasData} />
        ) : (
          <SuccessState
            ioData={ioData}
            sfData={sfData}
            groupedData={groupedData}
            displayDate={displayDate}
            ioError={ioError}
            sfError={sfError}
            ioErr={ioErr}
            sfErr={sfErr}
            date={date}
          />
        )}
      </div>
    </div>
  )
}

// ———————————————————————— UI STATES ————————————————————————
function LoadingState() {
  return (
    <div className="flex h-64 items-center justify-center">
      <div className="text-center">
        <Loader2 className="mx-auto h-8 w-8 animate-spin text-blue-600" />
        <p className="mt-2 text-gray-600 dark:text-gray-400">Loading sales fulfilment data...</p>
      </div>
    </div>
  )
}

function EmptyOrErrorState({
  ioError,
  sfError,
  ioErr,
  sfErr,
  hasData,
}: {
  ioError: boolean
  sfError: boolean
  ioErr: Error | null
  sfErr: Error | null
  hasData: boolean
}) {
  if (hasData) {
    return (
      <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
        <p className="text-sm text-yellow-800 dark:text-yellow-200">
          Warning: Partial data loaded. IO/RE/PS: {ioError ? ioErr?.message : 'OK'}, Sales Force: {sfError ? sfErr?.message : 'OK'}
        </p>
      </div>
    )
  }

  return (
    <div className="flex h-64 items-center justify-center">
      <div className="text-center">
        <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <p className="mt-2 text-gray-600 dark:text-gray-400">
          {ioError || sfError ? `Error: ${(ioErr || sfErr)?.message}` : 'No data found for the selected filters.'}
        </p>
      </div>
    </div>
  )
}

function SuccessState({
  ioData,
  sfData,
  groupedData,
  displayDate,
  ioError,
  sfError,
  ioErr,
  sfErr,
  date
}: {
  ioData: SalesFulfilmentResponseData[]
  sfData: SalesForceResponseData[]
  groupedData: Record<string, SalesFulfilmentResponseData[]>
  displayDate: string
  ioError: boolean
  sfError: boolean
  ioErr: Error | null
  sfErr: Error | null
  date?: string
}) {
  return (
    <div className="space-y-12">
      {/* Partial Error Warning */}
      {(ioError || sfError) && (
        <div className="bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-800 rounded-lg p-4">
          <p className="text-sm text-yellow-800 dark:text-yellow-200">
            Warning: Partial data loaded. IO/RE/PS: {ioError ? ioErr?.message : 'OK'}, Sales Force: {sfError ? sfErr?.message : 'OK'}
          </p>
        </div>
      )}

      <SectionCards data={ioData} />

      {/* IO-RE-PS & Brownfield/Greenfield */}
      <section className="space-y-6">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 dark:text-white">IO-RE-PS & Brownfield Greenfield</h2>
          <p className="text-gray-600 dark:text-gray-400 mt-1">Input Order, RE, and Put in Service • Brownfield & Greenfield expansion</p>
        </div>

        <div className="grid grid-cols-5 gap-4">
          <div className="col-span-3 text-center">
            <h2 className="text-lg font-semibold text-gray-800 border-b-2 border-blue-500 pb-1">
              IO-RE-PS
            </h2>
          </div>
          <div className="col-span-2 text-center">
            <h2 className="text-lg font-semibold text-gray-800 border-b-2 border-green-500 pb-1">
              Brownfield Greenfield
            </h2>
          </div>
        </div>

        <div className="grid grid-cols-5 gap-4">
          {Object.entries(groupedData).map(([level, items]) => (
            <LevelCharts key={level} level={level} items={items} displayDate={displayDate} />
          ))}
        </div>
      </section>

      {/* Analytics Section */}
      <section className="space-y-6">
        <div className="border-b border-gray-200 dark:border-gray-700 pb-4">
          <h2 className="text-2xl font-bold text-gray-900 dark:text-white">PS by Channel & SF • Funneling Group & WO by LoS</h2>
        </div>

        <div className="flex-1 grid lg:grid-cols-4 gap-6">
          <ChartSection title="PS By Channel" borderColor="border-blue-500">
            <ChartPie date={displayDate} data={ioData} />
          </ChartSection>

          <ChartSection title="Sales Force" borderColor="border-orange-500">
            <SalesForce data={sfData} selectedDate={date ? parseISO(date) : subDays(new Date(), 2)} />
          </ChartSection>

          <ChartSection title="Funneling Group" borderColor="border-green-500">
            <FunnelingGroup data={ioData} selectedDate={date ? parseISO(date) : subDays(new Date(), 2)} />
          </ChartSection>

          <ChartSection title="WO Follow Up" borderColor="border-purple-500">
            <WOLoS data={ioData} selectedDate={date ? parseISO(date) : subDays(new Date(), 2)} />
          </ChartSection>
        </div>
      </section>

      <FalloutDetail data={ioData} selectedDate={date ? parseISO(date) : subDays(new Date(), 2)} />
    </div>
  )
}

// ———————————————————————— HELPER COMPONENTS ————————————————————————
function LevelCharts({ level, items, displayDate }: { level: string; items: SalesFulfilmentResponseData[]; displayDate: string }) {
  if (!items.length) return null

  return (
    <>
      <ProgressCard title="IO" date={displayDate} data={mapIO(items)} />
      <ProgressCard title="RE" date={displayDate} data={mapRE(items)} />
      <ProgressCard title="PS" date={displayDate} data={mapPS(items)} />

      <ProgressCard title="Brownfield" date={displayDate} data={mapBrownfield(items)} />
      <ProgressCard title="Greenfield" date={displayDate} data={mapGreenfield(items)} />

    </>
  )
}

function ChartSection({ title, borderColor, children }: { title: string; borderColor: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col">
      <div className="text-center mb-4">
        <h3 className={`text-lg font-semibold text-gray-800 ${borderColor} border-b-2 pb-1`}>{title}</h3>
      </div>
      <div className="flex-1">{children}</div>
    </div>
  )
}

// ———————————————————————— MAPPERS ————————————————————————
const mapIO = (items: SalesFulfilmentResponseData[]) => items.map(i => ({
  territory: i.name,
  target: i.target_all_sales,
  actual: i.io_m,
  drr: i.drr_io,
  color: 'bg-chart-1 text-chart-1',
  ach_fm: `${(i.io_m / i.target_all_sales * 100).toFixed(2)}%`,
}))

const mapRE = (items: SalesFulfilmentResponseData[]) => items.map(i => ({
  territory: i.name,
  target: i.target_all_sales,
  actual: i.re_m,
  drr: i.drr_re,
  color: 'bg-chart-2 text-chart-2',
  ach_fm: `${(i.re_m / i.target_all_sales * 100).toFixed(2)}%`,
}))

const mapPS = (items: SalesFulfilmentResponseData[]) => items.map(i => ({
  territory: i.name,
  target: i.target_all_sales,
  actual: i.ps_m,
  drr: i.drr_ps,
  color: 'bg-chart-4 text-chart-4',
  ach_fm: `${(i.ps_m / i.target_all_sales * 100).toFixed(2)}%`,
}))

const mapBrownfield = (items: SalesFulfilmentResponseData[]) => items.map(i => ({
  territory: i.name,
  target: i.target_brownfield,
  actual: i.brownfield,
  drr: parseFloat(i.drr_brownfield.replace('%', '')),
  color: 'bg-orange-800 text-orange-800',
  ach_fm: `${(i.brownfield / i.target_brownfield * 100).toFixed(2)}%`,
}))

const mapGreenfield = (items: SalesFulfilmentResponseData[]) => items.map(i => ({
  territory: i.name,
  target: i.target_greenfield,
  actual: i.greenfield,
  drr: parseFloat(i.drr_greenfield.replace('%', '')),
  color: 'bg-teal-500 text-teal-500',
  ach_fm: `${(i.greenfield / i.target_greenfield * 100).toFixed(2)}%`,
}))

type SalesForceResponseData = {
  name: string | null;
  sf_black: number;
  sf_bronze: number;
  sf_silver: number;
  sf_gold: number;
  sf_platinum: number;
  total_sf: number;
}