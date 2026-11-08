export function formatTime(value: string): string {
  return new Date(value).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  });
}

const usd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 4,
});

/** Integer micro-USD as dollars; amounts under a hundredth of a cent say so. */
export function formatMicroUsd(microusd: number): string {
  if (microusd > 0 && microusd < 100) {
    return "< $0.0001";
  }
  return usd.format(microusd / 1_000_000);
}

export function formatCount(value: number): string {
  return value.toLocaleString("en-US");
}
