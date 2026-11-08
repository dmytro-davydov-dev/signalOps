// Structural checks for API responses: a response that fails one is rejected, not guessed at.

export type Checks = Record<string, (value: unknown) => boolean>;

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export const isString = (value: unknown) => typeof value === "string";
export const isNumber = (value: unknown) => typeof value === "number";
export const isBoolean = (value: unknown) => typeof value === "boolean";
export const isNullableString = (value: unknown) => value === null || typeof value === "string";
export const isOneOf = (options: readonly string[]) => (value: unknown) =>
  typeof value === "string" && options.includes(value);
export const isListOf = (check: (value: unknown) => boolean) => (value: unknown) =>
  Array.isArray(value) && value.every(check);
export const matches = (checks: Checks) => (value: unknown) =>
  isRecord(value) && Object.entries(checks).every(([key, check]) => check(value[key]));
export const isNullableNumber = (value: unknown) => value === null || typeof value === "number";
