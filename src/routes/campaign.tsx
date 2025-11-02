import { useCurrentSession } from '@/hooks/use-current-session'
import { createFileRoute, Outlet, useNavigate } from '@tanstack/react-router'

import { Separator } from "@/components/ui/separator"
import {
    SidebarInset,
    SidebarProvider,
    SidebarTrigger,
} from "@/components/ui/sidebar"
import { AppSidebar } from './campaign/-components/app-sidebar'

export const Route = createFileRoute('/campaign')({
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
            <AppSidebar />
            <SidebarInset>
                <header className="flex h-16 shrink-0 items-center gap-2 transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
                    <div className="flex items-center gap-2 px-4">
                        <SidebarTrigger className="-ml-1" />
                        <Separator
                            orientation="vertical"
                            className="mr-2 data-[orientation=vertical]:h-4"
                        />
                    </div>
                </header>
                <div className="flex flex-1 flex-col gap-4 p-4 pt-0">
                    <Outlet />
                </div>
            </SidebarInset>
        </SidebarProvider>
    )
}
