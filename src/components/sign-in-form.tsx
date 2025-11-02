import { useForm } from "@tanstack/react-form";
import { toast } from "sonner";
import { HTTPException } from "hono/http-exception";
import { useNavigate } from "@tanstack/react-router";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { z } from "zod";
import { signIn } from "@hono/auth-js/react"

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"

type ResponseType = { message: string }
const LoginSchema = z.object({
    username: z.string().trim().min(1, 'Please enter your username'),
    password: z.string().min(1, "Please enter your password"),
})

export default function SignInForm() {
    const navigate = useNavigate();
    const queryClient = useQueryClient();

    const mutation = useMutation<ResponseType, HTTPException, z.infer<typeof LoginSchema>>({
        mutationFn: async (values) => {
            const response = await signIn('credentials', {
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
                        className="space-y-4"
                    >
                        <div>
                            <form.Field name="username">
                                {(field) => (
                                    <div className="space-y-2">
                                        <Label htmlFor={field.name}>Username</Label>
                                        <Input
                                            id={field.name}
                                            name={field.name}
                                            type="text"
                                            value={field.state.value}
                                            onBlur={field.handleBlur}
                                            onChange={(e) => field.handleChange(e.target.value)}
                                        />
                                        {field.state.meta.errors.map((error) => (
                                            <p key={error?.message} className="text-red-500">
                                                {error?.message}
                                            </p>
                                        ))}
                                    </div>
                                )}
                            </form.Field>
                        </div>

                        <div>
                            <form.Field name="password">
                                {(field) => (
                                    <div className="space-y-2">
                                        <Label htmlFor={field.name}>Password</Label>
                                        <Input
                                            id={field.name}
                                            name={field.name}
                                            type="password"
                                            value={field.state.value}
                                            onBlur={field.handleBlur}
                                            onChange={(e) => field.handleChange(e.target.value)}
                                        />
                                        {field.state.meta.errors.map((error) => (
                                            <p key={error?.message} className="text-red-500">
                                                {error?.message}
                                            </p>
                                        ))}
                                    </div>
                                )}
                            </form.Field>
                        </div>

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
                    </form>
                </CardContent>
            </Card>
        </div>
    );
}
