import { afterEach, expect, it, vi } from "vite-plus/test";
import { EnvironmentId } from "@t3tools/contracts";
import { AsyncResult } from "effect/reactivity";
import {
  markdownImageGallery,
  markdownImageItems,
  resolveMarkdownImageGallery,
} from "./markdownImageGallery";
import { resolveMarkdownMediaPreview, wrapExpandedImageIndex } from "./ExpandedImagePreview";

afterEach(() => document.body.replaceChildren());

it.each([true, false])(
  "keeps linked private images authenticated when navigating from a linked=%s image",
  async (linked) => {
    const base =
      "https://dev.azure.com/org/project/_apis/git/repositories/repo/pullRequests/42/attachments";
    const urls = [`${base}/first.gif#preview`, `${base}/second.gif#preview`];
    const container = document.createElement("div");
    container.setAttribute("data-image-gallery", "");
    document.body.append(container);
    const images = urls.map((url, index) => {
      const parent = document.createElement(linked || index === 1 ? "a" : "span");
      if (parent instanceof HTMLAnchorElement) parent.href = url;
      const image = document.createElement("img");
      parent.append(image);
      container.append(parent);
      markdownImageItems.set(image, {
        src: `https://remote.example/thumb-${index}`,
        name: `Image ${index}`,
      });
      return image;
    });
    const environmentId = EnvironmentId.make("remote");
    const createAssetUrl = vi.fn<
      Parameters<typeof resolveMarkdownMediaPreview>[0]["createAssetUrl"]
    >(async ({ input }) =>
      AsyncResult.success({
        relativeUrl: `/api/assets/signed/${"url" in input.resource ? input.resource.url.split("/").pop() : "unexpected"}`,
        expiresAt: 2000,
      }),
    );
    const input = {
      source: urls[0]!,
      cwd: "/repo",
      environmentId,
      pullRequestMedia: true,
      httpBaseUrl: "https://remote.example",
      createAssetUrl,
    };
    const selected = (await resolveMarkdownMediaPreview(input))!.images[0]!;
    const gallery = await resolveMarkdownImageGallery(
      markdownImageGallery(images[0]!, selected),
      input,
    );
    const next = gallery.images[wrapExpandedImageIndex(gallery.index + 1, gallery.images.length)]!;
    expect(gallery.images).toHaveLength(2);
    expect(next.src).toBe("https://remote.example/api/assets/signed/second.gif#preview");
    expect(next.actionsSource?.asset).toEqual({
      environmentId,
      resource: {
        _tag: "azure-devops-media",
        cwd: "/repo",
        url: `${base}/second.gif`,
      },
    });
    expect(gallery.images[gallery.index]).toBe(selected);
    expect(createAssetUrl).toHaveBeenCalledTimes(2);
  },
);
