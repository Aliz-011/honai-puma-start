import 'dotenv/config';

console.log("Checking env vars...");
console.log("AUTH_SECRET length:", process.env.AUTH_SECRET?.length);
console.log("AUTH_SECRET starts with quote:", process.env.AUTH_SECRET?.startsWith('"'));
console.log("AUTH_SECRET ends with quote:", process.env.AUTH_SECRET?.endsWith('"'));
