import { SignUpForm } from '@/components/sign-up-form';
import { useCurrentSession } from '@/hooks/use-current-session';
import { createFileRoute, useNavigate } from '@tanstack/react-router'
import { GalleryVerticalEndIcon } from 'lucide-react';

export const Route = createFileRoute('/register')({
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

    if (session?.user) {
        navigate({ to: '/', replace: true });
    }
    return (
        <div className="flex min-h-svh flex-col items-center justify-center gap-6 bg-muted p-6 md:p-10">
            Hello, don't even try bitch.
        </div>
    )
}
