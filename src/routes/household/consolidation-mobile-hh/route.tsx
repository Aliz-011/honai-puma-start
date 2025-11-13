import { createFileRoute, Outlet } from '@tanstack/react-router'

export const Route = createFileRoute('/household/consolidation-mobile-hh')({
  component: RouteComponent,
})

function RouteComponent() {
  return <div>
    <Outlet />
  </div>
}
