import { Loader2 } from 'lucide-react'
import { DataTable, Revenue } from './data-table';

export function LoadingState() {
    return (
        <div className="flex h-full flex-col items-center justify-center space-y-3 py-12">
            <Loader2 className="h-8 w-8 animate-spin text-blue-600" />
            <p className="text-sm text-muted-foreground">Loading consolidation data...</p>
        </div>
    )
}

export function ErrorState({ error, hasData }: { error: unknown; hasData: boolean }) {
    if (hasData) {
        return (
            <div className='w-full text-center'>
                <p className="text-red-500">
                    Partial data loaded. Failed to fetch latest: {(error as Error)?.message}
                </p>
            </div>
        )
    }

    return (
        <div className="flex h-full flex-col items-center justify-center space-y-3 py-12">
            <p className="text-lg font-medium text-destructive">Failed to load data</p>
            <p className="max-w-md text-center text-sm text-muted-foreground">
                {(error as Error)?.message || 'An unexpected error occurred.'}
            </p>
        </div>
    )
}

export function EmptyState() {
    return (
        <div className="flex h-full flex-col items-center justify-center space-y-3 py-12">
            <p className="text-lg font-medium text-muted-foreground">No data found</p>
            <p className="max-w-md text-center text-sm text-muted-foreground">
                Try adjusting the filters or date range.
            </p>
        </div>
    )
}

export function SuccessState({ data, isLoading, title }: { data: Revenue[]; title: string; isLoading: boolean }) {
    return (
        <DataTable
            data={data}
            title={title}
            isLoading={isLoading}
        />
    )
}