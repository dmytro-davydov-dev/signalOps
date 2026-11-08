import { describe, expect, it } from "vitest";
import { parseSse } from "./progressStream";

describe("parseSse", () => {
  it("parses complete messages and keeps a partial one for the next chunk", () => {
    const first = parseSse('retry: 3000\n\nid: 7\nevent: progress\ndata: {"a"');
    expect(first.messages).toEqual([{ id: undefined, event: "message", data: "", retry: 3000 }]);

    const second = parseSse(`${first.rest}:1}\n\n`);
    expect(second.messages).toEqual([
      { id: "7", event: "progress", data: '{"a":1}', retry: undefined },
    ]);
    expect(second.rest).toBe("");
  });

  it("skips keep-alive comments", () => {
    expect(parseSse(": keepalive\n\n").messages).toEqual([]);
  });

  it("reads CRLF line endings and joins multi-line data", () => {
    const { messages } = parseSse("event: reset\r\ndata: one\r\ndata: two\r\n\r\n");
    expect(messages).toEqual([{ id: undefined, event: "reset", data: "one\ntwo", retry: undefined }]);
  });

  it("ignores a malformed retry and unknown fields", () => {
    const { messages } = parseSse("retry: soon\nfoo: bar\ndata: x\n\n");
    expect(messages).toEqual([{ id: undefined, event: "message", data: "x", retry: undefined }]);
  });
});
