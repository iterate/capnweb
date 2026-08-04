import { execFileSync } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

interface NpmPackResult {
  files: Array<{ path: string }>;
}

const repositoryRoot = dirname(dirname(fileURLToPath(import.meta.url)));

test("the npm package carries the buildable C peer without build artifacts", () => {
  const output = execFileSync(
    "npm",
    ["pack", "--dry-run", "--json", "--ignore-scripts"],
    {
      cwd: repositoryRoot,
      encoding: "utf8",
    },
  );
  const [pack] = JSON.parse(output) as NpmPackResult[];
  const paths = new Set(pack?.files.map(({ path }) => path));

  expect(paths).toContain("c/CMakeLists.txt");
  expect(paths).toContain("c/include/capnweb/capnweb.h");
  expect(paths).toContain("c/src/session.c");
  expect([...paths].filter((path) => path.startsWith("c/build"))).toEqual([]);
});
