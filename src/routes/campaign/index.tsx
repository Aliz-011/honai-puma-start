import { createFileRoute } from '@tanstack/react-router'
import {
    createStandardSchemaV1,
    parseAsString
} from 'nuqs'

import { Filter } from './-components/-wl/filter'

const searchParams = {
    branch: parseAsString.withDefault(''),
    cluster: parseAsString.withDefault(''),
    kabupaten: parseAsString.withDefault(''),
    kecamatan: parseAsString.withDefault(''),
    method: parseAsString.withDefault('wa'),
    product_offer: parseAsString.withDefault('slm_lifestage_3'),
}

export const Route = createFileRoute('/campaign/')({
    component: RouteComponent,
    validateSearch: createStandardSchemaV1(searchParams, {
        partialOutput: true
    })
})

function RouteComponent() {
    return (
        <div className='relative space-y-4'>
            <h3 className='text-xl font-bold'>WL Campaign</h3>
            <Filter />
        </div>
    )
}
