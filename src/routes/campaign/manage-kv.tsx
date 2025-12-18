import { createFileRoute } from '@tanstack/react-router'
import { useForm } from '@tanstack/react-form'
import { useMutation } from '@tanstack/react-query'
import { toast } from 'sonner'
import * as z from 'zod/v4'

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    CardDescription,
} from "@/components/ui/card"
import { Field, FieldLabel, FieldError, FieldGroup } from "@/components/ui/field"

export const Route = createFileRoute('/campaign/manage-kv')({
    component: ManageKVPage,
})

function ManageKVPage() {
    const uploadMutation = useMutation({
        mutationFn: async (file: File) => {
            const formData = new FormData()
            formData.append('file', file)
            const res = await fetch('/api/campaign/kv/upload', {
                method: 'POST',
                body: formData,
            })
            if (!res.ok) throw new Error('Upload failed')
            return res.json() as Promise<{ success: boolean, path: string }>
        },
    })

    const saveMutation = useMutation({
        mutationFn: async (data: { title: string, imagePath: string, waWording: string, smsWording: string }) => {
            const res = await fetch('/api/campaign/kv', {
                method: 'POST',
                body: JSON.stringify(data),
                headers: { 'Content-Type': 'application/json' }
            })
            if (!res.ok) throw new Error('Save failed')
            return res.json()
        }
    })

    const form = useForm({
        defaultValues: {
            title: '',
            image: undefined as File | undefined,
            waWording: '',
            smsWording: ''
        },
        onSubmit: async ({ value }) => {
            try {
                let imagePath = ''
                if (value.image) {
                    const uploadRes = await uploadMutation.mutateAsync(value.image)
                    imagePath = uploadRes.path
                } else {
                    // Start of fallback logic if no new image uploaded (optimization/editing logic could go here)
                    throw new Error("Image is required for new campaign")
                }

                await saveMutation.mutateAsync({
                    title: value.title,
                    imagePath,
                    waWording: value.waWording,
                    smsWording: value.smsWording
                })

                toast.success('KV & Wording saved successfully')
                form.reset()
            } catch (error) {
                toast.error('Failed to save data')
                console.error(error)
            }
        },
        validators: {
            onSubmit: z.object({
                title: z.string().min(1, "Title is required"),
                image: z.instanceof(File, { message: "Image is required" }),
                waWording: z.string().min(1, "WA Wording is required"),
                smsWording: z.string().min(1, "SMS Wording is required"),
            })
        }
    })

    return (
        <div className="p-4 w-full max-w-5xl mx-auto">
            <Card>
                <CardHeader>
                    <CardTitle>Manage Campaign KV & Wording</CardTitle>
                    <CardDescription>Upload image and set wording for WhatsApp and SMS.</CardDescription>
                </CardHeader>
                <CardContent>
                    <form
                        onSubmit={(e) => {
                            e.preventDefault()
                            e.stopPropagation()
                            void form.handleSubmit()
                        }}
                    >
                        <FieldGroup>
                            <form.Field name="title">
                                {(field) => {
                                    const isInvalid = field.state.meta.isTouched && !field.state.meta.isValid
                                    return (
                                        <Field data-invalid={isInvalid}>
                                            <FieldLabel htmlFor={field.name}>Title</FieldLabel>
                                            <Input
                                                id={field.name}
                                                name={field.name}
                                                value={field.state.value}
                                                onBlur={field.handleBlur}
                                                onChange={(e) => field.handleChange(e.target.value)}
                                            />
                                            {isInvalid && (
                                                <FieldError>{field.state.meta.errors[0]?.message}</FieldError>
                                            )}
                                        </Field>
                                    )
                                }}
                            </form.Field>

                            <form.Field name="image">
                                {(field) => {
                                    const isInvalid = field.state.meta.isTouched && !field.state.meta.isValid
                                    // Manually tracking logic could be needed for file input, but checking simple valid state for now
                                    // Note: TanStack Form file handling can be tricky with value/onChange binding
                                    return (
                                        <Field data-invalid={isInvalid}>
                                            <FieldLabel htmlFor={field.name}>Image</FieldLabel>
                                            <Input
                                                id={field.name}
                                                name={field.name}
                                                type="file"
                                                accept="image/*"
                                                onChange={(e) => {
                                                    const file = e.target.files?.[0]
                                                    field.handleChange(file)
                                                }}
                                            />
                                            {isInvalid && (
                                                <FieldError>{field.state.meta.errors[0]?.message}</FieldError>
                                            )}
                                        </Field>
                                    )
                                }}
                            </form.Field>

                            <form.Field name="waWording">
                                {(field) => {
                                    const isInvalid = field.state.meta.isTouched && !field.state.meta.isValid
                                    return (
                                        <Field data-invalid={isInvalid}>
                                            <FieldLabel htmlFor={field.name}>WA Wording</FieldLabel>
                                            <Textarea
                                                id={field.name}
                                                name={field.name}
                                                value={field.state.value}
                                                onBlur={field.handleBlur}
                                                onChange={(e) => field.handleChange(e.target.value)}
                                                rows={5}
                                            />
                                            {isInvalid && (
                                                <FieldError>{field.state.meta.errors[0]?.message}</FieldError>
                                            )}
                                        </Field>
                                    )
                                }}
                            </form.Field>

                            <form.Field name="smsWording">
                                {(field) => {
                                    const isInvalid = field.state.meta.isTouched && !field.state.meta.isValid
                                    return (
                                        <Field data-invalid={isInvalid}>
                                            <FieldLabel htmlFor={field.name}>SMS Wording</FieldLabel>
                                            <Textarea
                                                id={field.name}
                                                name={field.name}
                                                value={field.state.value}
                                                onBlur={field.handleBlur}
                                                onChange={(e) => field.handleChange(e.target.value)}
                                                rows={3}
                                            />
                                            {isInvalid && (
                                                <FieldError>{field.state.meta.errors[0]?.message}</FieldError>
                                            )}
                                        </Field>
                                    )
                                }}
                            </form.Field>

                            <form.Subscribe>
                                {(state) => (
                                    <Button
                                        type="submit"
                                        className="w-full"
                                        disabled={state.isSubmitting || uploadMutation.isPending || saveMutation.isPending}
                                    >
                                        {(state.isSubmitting || uploadMutation.isPending || saveMutation.isPending) ? "Saving..." : "Save Changes"}
                                    </Button>
                                )}
                            </form.Subscribe>
                        </FieldGroup>
                    </form>
                </CardContent>
            </Card>
        </div>
    )
}
