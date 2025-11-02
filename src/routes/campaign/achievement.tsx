import { createFileRoute } from '@tanstack/react-router'
import {
  createStandardSchemaV1,
  parseAsString
} from 'nuqs'

import { DataTable } from './-components/-achievement/data-table'
import { Filter } from './-components/-achievement/filter'
import { client } from '@/lib/client'

const searchParams = {
  branch: parseAsString.withDefault(''),
  cluster: parseAsString.withDefault(''),
  kabupaten: parseAsString.withDefault('')
}

export const Route = createFileRoute('/campaign/achievement')({
  component: RouteComponent,
  validateSearch: createStandardSchemaV1(searchParams, {
    partialOutput: true
  }),
  loaderDeps: ({ search: { branch, cluster, kabupaten } }) => ({ branch, cluster, kabupaten }),
  loader: async ({ deps: { branch, cluster, kabupaten } }) => {
    const response = await client.api.campaign['campaign-achievement'].$get({ query: { branch, cluster, kabupaten } })

    return response.json()
  }
})

function RouteComponent() {
  return <div className='space-y-4'>
    <Filter />
    <DataTable />
  </div>
}
