import { createFileRoute, Link, Outlet, useNavigate } from '@tanstack/react-router'
import { useCurrentSession } from '@/hooks/use-current-session'

import { SidebarInset, SidebarProvider, SidebarTrigger } from '@/components/ui/sidebar'
import { AppSidebar } from './household/-components/app-sidebar'
import { Separator } from '@/components/ui/separator'
import { Button } from '@/components/ui/button'

export const Route = createFileRoute('/household')({
    component: RouteComponent,
})

function RouteComponent() {
    const navigate = useNavigate();
    const { data: session, isLoading } = useCurrentSession();

    if (isLoading) {
        return (
            <main className='max-w-screen-2xl mx-auto w-full h-[80vh] pt-5'>
                <div className='flex flex-1 gap-8 items-center justify-center h-full'>Loading...</div>
            </main>
        )
    }

    if (!session?.user) {
        return navigate({ to: '/login', replace: true });
    }

    return (
        <SidebarProvider>
            <AppSidebar variant='inset' />
            <SidebarInset>
                <header className="group-has-data-[collapsible=icon]/sidebar-wrapper:h-12 flex h-12 shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear">
                    <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
                        <SidebarTrigger className="-ml-1" />
                        <Separator
                            orientation="vertical"
                            className="mx-2 data-[orientation=vertical]:h-4"
                        />
                        <Button variant='ghost' size='sm' asChild>
                            <Link to='/' className="font-medium">Home</Link>
                        </Button>
                    </div>
                </header>

                <div className="flex flex-1 flex-col">
                    <div className="@container/main flex flex-1 flex-col gap-2">
                        <div className="flex flex-col gap-4 py-4 md:gap-6 md:py-6">
                            <div className="px-4 lg:px-6">
                                <Outlet />
                            </div>
                        </div>
                    </div>
                </div>
            </SidebarInset>
        </SidebarProvider>
    )
}
