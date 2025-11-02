import Header from '@/components/Header'
import { useCurrentSession } from '@/hooks/use-current-session'
import { useNavigate } from '@tanstack/react-router'
import { Link } from '@tanstack/react-router'
import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/')({
  component: App,
})

const LINKS = [
  { to: '/puma', label: 'Mobile' },
  { to: '/household', label: 'Household' },
  { to: '/campaign', label: 'Campaign' },
] as const

function App() {
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
    <>
      <Header />
      <main className="max-w-screen-2xl mx-auto w-full h-[80vh] pt-5" role="main">
        <div className="flex flex-1 gap-8 items-center justify-center h-full">
          {LINKS.map(({ to, label }) => (
            <Link
              key={to}
              className="font-semibold text-lg capitalize hover:underline focus-visible:outline focus-visible:outline-blue-500 rounded"
              to={to}
            >
              {label}
            </Link>
          ))}
        </div>
      </main>
    </>
  )
}
