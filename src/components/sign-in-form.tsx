import { useForm } from "@tanstack/react-form";
import { toast } from "sonner";
import { HTTPException } from "hono/http-exception";
import { useNavigate } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import * as z from "zod/v4";
import { useState } from "react";
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
import { Select, SelectTrigger, SelectValue, SelectContent, SelectItem } from "@/components/ui/select";
import { Field, FieldLabel, FieldError, FieldGroup } from "@/components/ui/field";

type ResponseType = { message: string }
type AuthMethod = 'credentials' | 'ldap'

const LoginSchema = z.object({
    username: z.string().trim().min(1, 'Please enter your username'),
    password: z.string().min(1, "Please enter your password"),
})

export default function SignInForm() {
    const [authMethod, setAuthMethod] = useState<AuthMethod>('ldap')
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    const mutation = useMutation<ResponseType, HTTPException, z.infer<typeof LoginSchema>>({
        mutationFn: async (values) => {
            const response = await signIn(authMethod, {
                redirect: false,
                callbackUrl: '/',
                ...values
            })

            if (response?.error) {
                throw new Error(response.error || 'Failed to sign in')
            }

            return { message: 'Success' }
        }
    })

    const form = useForm({
        defaultValues: {
            username: "",
            password: "",
        },
        onSubmit: async ({ value }) => {
            mutation.mutate({ ...value }, {
                onSuccess: async () => {
                    toast.success("Sign in successful");
                    console.log(authMethod);

                    await new Promise(resolve => setTimeout(resolve, 500));

                    await queryClient.invalidateQueries({ queryKey: ['current-session'] })

                    navigate({ replace: true, to: '/' })
                },
                onError: (error) => {
                    toast.error(error.message || 'Failed, please try again.');
                },
            })
        },
        validators: {
            onSubmit: z.object({
                username: z.string().trim().min(1, 'Please enter your username'),
                password: z.string().min(6, "Password must be at least 6 characters"),
            }),
        },
    });

    return (
        <div className="flex flex-col gap-6">
            <Card>
                <CardHeader className="text-center">
                    <CardTitle className="text-xl">Welcome back</CardTitle>
                    <CardDescription>
                        Login with your username and password to continue.
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
                                <Select value={authMethod} onValueChange={e => setAuthMethod(e as AuthMethod)}>
                                    <SelectTrigger>
                                        <SelectValue placeholder="LDAP" />
                                    </SelectTrigger>
                                    <SelectContent>
                                        <SelectItem value="ldap">LDAP</SelectItem>
                                        <SelectItem value="credentials">Regular</SelectItem>
                                    </SelectContent>
                                </Select>
                            </Field>


                            <Field>
                                <form.Subscribe>
                                    {(state) => (
                                        <Button
                                            type="submit"
                                            className="w-full"
                                            disabled={!state.canSubmit || state.isSubmitting || mutation.isPending}
                                        >
                                            {state.isSubmitting ? "Submitting..." : "Sign In"}
                                        </Button>
                                    )}
                                </form.Subscribe>
                            </Field>
                        </FieldGroup>
                    </form>
                </CardContent>
            </Card>
        </div>
    );
}
