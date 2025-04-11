// async function delay(ms: number): Promise<void> {
//     return new Promise( resolve => setTimeout(resolve, ms) );
// }
//
// async function profile() {
//     const profiler = new Profiler();

//     await profiler.profileAsync("test", () => delay(1000));
//     await profiler.profileAsync("test2", () => delay(500));
//     await profiler.profileAsync("test3", () => delay(100));

//     profiler.report();
// }
// ┌─────────┬─────────┬───────────┬─────────┐
// │ (index) │ profile │ time (ms) │ percent │
// ├─────────┼─────────┼───────────┼─────────┤
// │ 0       │ 'test'  │ 1000.64   │ 0.62    │
// │ 1       │ 'test2' │ 500.86    │ 0.31    │
// │ 2       │ 'test3' │ 100.4     │ 0.06    │
// └─────────┴─────────┴───────────┴─────────┘

export class Profiler {
  private map: Map<string, number> = new Map<string, number>();
  constructor() {}

  profile<T>(key: string, fn: () => T): T {
    const start = performance.now();
    const result = fn();
    const end = performance.now();

    const prev = this.map.get(key);
    this.map.set(key, end - start + (prev ?? 0));

    return result;
  }

  async profileAsync<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const start = performance.now();
    const result = await fn();
    const end = performance.now();

    const prev = this.map.get(key);
    this.map.set(key, end - start + (prev ?? 0));

    return result;
  }

  report<T>(additional?: (time: number) => T) {
    const report = [];
    let sum = 0;
    for (const entry of this.map) {
      sum += entry[1];
    }
    for (const entry of this.map) {
      report.push({
        profile: entry[0],
        "time (ms)": Math.round(entry[1] * 100) / 100,
        percent: Math.round((entry[1] / sum) * 100) / 100,
        ...(additional?.(Math.round(entry[1] * 100) / 100) ?? {}),
      });
    }
    console.table(report);
  }
}
