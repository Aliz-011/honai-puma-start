import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { GalleryVerticalEndIcon } from 'lucide-react'
import SignInForm from '@/components/sign-in-form'
import { useCurrentSession } from '@/hooks/use-current-session'

export const Route = createFileRoute('/login')({
    component: RouteComponent
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

    if (session?.user) {
        navigate({ to: '/', replace: true });
    }

    return (
        <div className="flex min-h-svh flex-col items-center justify-center gap-6 bg-muted p-6 md:p-10">
            <div className="flex w-full max-w-sm flex-col gap-6">
                <a href="#" className="flex items-center gap-2 self-center font-medium">
                    <div className="flex h-6 w-6 items-center justify-center rounded-md bg-primary text-primary-foreground">
                        <GalleryVerticalEndIcon className="size-4" />
                    </div>
                    Honai PUMA.
                </a>

                <SignInForm />
            </div>
        </div>
    )
}
