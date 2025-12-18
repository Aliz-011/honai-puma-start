import { useForm } from "@tanstack/react-form";
import { toast } from "sonner";
import { useRouter } from "@tanstack/react-router";
import { useQueryClient } from "@tanstack/react-query";
import * as z from "zod/v4";
import { signIn } from "@hono/auth-js/react"

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import { Field, FieldLabel, FieldError, FieldGroup } from "@/components/ui/field";
import { useServerFn } from "@tanstack/react-start";
import { createUser } from "@/data/user";

const RegisterSchema = z.object({
    username: z.string().trim().min(1, 'Please enter your username'),
    password: z.string().min(1, "Please enter your password"),
})

export const SignUpForm = () => {
    const router = useRouter();
    const queryClient = useQueryClient();
    const mutation = useServerFn(createUser)

    const form = useForm({
        defaultValues: {
            username: "",
            password: "",
        },
        validators: {
            onSubmit: RegisterSchema,
        },
        onSubmit: async ({ value }) => {
            const res = await mutation({
                data: {
                    ...value,
                    email: `${value.username}@honai-puma.com`,
                    name: value.username,
                }
            })

            if (!res.id) {
                throw new Error('Failed to create user')
            }

            toast.success('User created successfully')

            await new Promise(resolve => setTimeout(resolve, 500));

            await signIn('credentials', {
                redirect: false,
                callbackUrl: '/',
                ...value
            })

            await queryClient.invalidateQueries({ queryKey: ['current-session'] })

            router.invalidate()
        }
    })

    return (
        <div className="flex flex-col gap-6">
            <Card>
                <CardHeader className="text-center">
                    <CardTitle className="text-xl">Welcome to Honai PUMA</CardTitle>
                    <CardDescription>
                        Create an account to continue.
                    </CardDescription>
                </CardHeader>

                <CardContent>
                    <form
                        onSubmit={(e) => {
                            e.preventDefault();
                            e.stopPropagation();
                            void form.handleSubmit();
                        }}
                    >
                        <FieldGroup>
                            <form.Field name="username">
                                {(field) => {
                                    const isInvalid =
                                        field.state.meta.isTouched && !field.state.meta.isValid
                                    return (
                                        <Field data-invalid={isInvalid}>
                                            <FieldLabel htmlFor={field.name}>Username</FieldLabel>
                                            <Input
                                                id={field.name}
                                                name={field.name}
                                                type="text"
                                                value={field.state.value}
                                                onBlur={field.handleBlur}
                                                onChange={(e) => field.handleChange(e.target.value)}
                                            />
                                            {isInvalid && (
                                                <FieldError>
                                                    {field.state.meta.errors[0]?.message}
                                                </FieldError>
                                            )}
                                        </Field>
                                    )
                                }}
                            </form.Field>

                            <form.Field name="password">
                                {(field) => {
                                    const isInvalid =
                                        field.state.meta.isTouched && !field.state.meta.isValid
                                    return (
                                        <Field data-invalid={isInvalid}>
                                            <FieldLabel htmlFor={field.name}>Password</FieldLabel>
                                            <Input
                                                id={field.name}
                                                name={field.name}
                                                type="password"
                                                value={field.state.value}
                                                onBlur={field.handleBlur}
                                                onChange={(e) => field.handleChange(e.target.value)}
                                            />
                                            {isInvalid && (
                                                <FieldError>
                                                    {field.state.meta.errors[0]?.message}
                                                </FieldError>
                                            )}
                                        </Field>
                                    )
                                }}
                            </form.Field>

                            <Field>
                                <form.Subscribe>
                                    {(state) => (
                                        <Button
                                            type="submit"
                                            className="w-full"
                                            disabled={!state.canSubmit || state.isSubmitting}
                                        >
                                            {state.isSubmitting ? "Submitting..." : "Sign Up"}
                                        </Button>
                                    )}
                                </form.Subscribe>
                            </Field>
                        </FieldGroup>
                    </form>
                </CardContent>
            </Card>
        </div>
    )
}
