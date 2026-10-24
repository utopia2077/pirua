import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { AcpClient, RpcError } from "../src/acp/client.ts";
import { TuiRuntimeHost } from "../src/session/host.ts";
import { ClientSettings } from "../src/ui/settings.ts";
import type { AuthEvent } from "../src/display/types.ts";

function host() {
  const client = new AcpClient(new PassThrough(), new PassThrough());
  return new TuiRuntimeHost(client, {
    cwd: "/unused",
    settings: new ClientSettings("/unused/auth-ui.json"),
  });
}
const notification = (event?: AuthEvent) => ({
  mode: "form",
  message:
    "Visit https://auth.example.invalid/authorize and follow the instructions",
  requestedSchema: { type: "object", properties: {}, required: [] },
  ...(event ? { _meta: { "pirua.dev/auth": { event } } } : {}),
});

describe("native auth notification elicitation", () => {
  it.each<AuthEvent>([
    {
      type: "auth_url",
      url: "https://auth.example.invalid/authorize",
      instructions: "Finish signing in",
    },
    {
      type: "device_code",
      verificationUri: "https://auth.example.invalid/device",
      userCode: "TEST-CODE",
    },
    {
      type: "info",
      message: "Follow the next step",
      links: [{ label: "Help", url: "https://auth.example.invalid/help" }],
    },
    { type: "progress", message: "Waiting for authentication" },
  ])(
    "presents the native $type event without adding a confirmation step",
    async (event) => {
      const runtime = host();
      const notify = vi.fn(),
        prompt = vi.fn();
      const confirm = vi.fn();
      runtime.authInteraction = {
        signal: new AbortController().signal,
        notify,
        prompt,
      };
      runtime.presentation.bindUI({ confirm } as any);
      try {
        await expect(
          runtime.client.onRequest!(
            "elicitation/create",
            notification(event),
            new AbortController().signal,
          ),
        ).resolves.toEqual({ action: "accept", content: {} });
        expect(notify).toHaveBeenCalledExactlyOnceWith(event);
        expect(prompt).not.toHaveBeenCalled();
        expect(confirm).not.toHaveBeenCalled();
      } finally {
        runtime.client.close();
      }
    },
  );

  it("shows a zero-field form message when auth metadata is absent", async () => {
    const runtime = host();
    const notify = vi.fn();
    runtime.presentation.bindUI({ notify } as any);
    try {
      await expect(
        runtime.client.onRequest!(
          "elicitation/create",
          notification(),
          new AbortController().signal,
        ),
      ).resolves.toEqual({ action: "accept", content: {} });
      expect(notify).toHaveBeenCalledExactlyOnceWith(
        notification().message,
        "info",
      );
    } finally {
      runtime.client.close();
    }
  });

  it("does not accept undisplayed instructions when no presentation is available", async () => {
    const runtime = host();
    try {
      await expect(
        runtime.client.onRequest!(
          "elicitation/create",
          notification(),
          new AbortController().signal,
        ),
      ).rejects.toBeInstanceOf(RpcError);
    } finally {
      runtime.client.close();
    }
  });

  it.each(["request", "login", "during notification"])(
    "honors %s cancellation",
    async (when) => {
      const runtime = host();
      const request = new AbortController(),
        login = new AbortController();
      const notify = vi.fn(() => {
        if (when === "during notification") login.abort();
      });
      runtime.authInteraction = {
        signal: login.signal,
        notify,
        prompt: vi.fn(),
      };
      if (when === "request") request.abort();
      if (when === "login") login.abort();
      try {
        await expect(
          runtime.client.onRequest!(
            "elicitation/create",
            notification({ type: "info", message: "Sign in" }),
            request.signal,
          ),
        ).resolves.toEqual({ action: "cancel" });
        expect(notify).toHaveBeenCalledTimes(
          when === "during notification" ? 1 : 0,
        );
      } finally {
        runtime.client.close();
      }
    },
  );

  it("keeps nonempty API-key forms and cancels subsequent manual input", async () => {
    const runtime = host();
    const login = new AbortController();
    const prompt = vi
      .fn()
      .mockResolvedValueOnce("synthetic-test-key")
      .mockImplementationOnce(() => {
        login.abort();
        throw new Error("Login cancelled");
      });
    runtime.authInteraction = { signal: login.signal, notify: vi.fn(), prompt };
    const form = {
      mode: "form",
      message: "API key",
      requestedSchema: {
        properties: { value: { type: "string", title: "Enter API key" } },
      },
    };
    try {
      await expect(
        runtime.client.onRequest!(
          "elicitation/create",
          form,
          new AbortController().signal,
        ),
      ).resolves.toEqual({
        action: "accept",
        content: { value: "synthetic-test-key" },
      });
      await expect(
        runtime.client.onRequest!(
          "elicitation/create",
          notification({
            type: "auth_url",
            url: "https://auth.example.invalid",
          }),
          new AbortController().signal,
        ),
      ).resolves.toEqual({ action: "accept", content: {} });
      await expect(
        runtime.client.onRequest!(
          "elicitation/create",
          form,
          new AbortController().signal,
        ),
      ).resolves.toEqual({ action: "cancel" });
      expect(prompt.mock.calls[0][0].message).toBe("Enter API key");
    } finally {
      runtime.client.close();
    }
  });
});
