// Copyright (c) 2026 Iterate
// Licensed under the MIT license found in the LICENSE.txt file or at:
//     https://opensource.org/license/mit

import type {
  SessionKnownFailure,
  SessionTestId,
} from "./session-battery.js";

export default {
  "deferred-promise-pipelining": {
    reason: "the embedded profile does not queue calls on unresolved results",
    expectedError: "CAPNWEB_E_UNSUPPORTED_PIPELINE",
  },
  "value-pipelining": {
    reason: "the embedded profile does not evaluate paths on returned values",
    expectedError: "CAPNWEB_E_UNSUPPORTED_PIPELINE",
  },
  blobs: {
    reason: "the embedded profile omits pipe and Blob streaming",
    expectedError: "CAPNWEB_E_UNSUPPORTED_PIPE",
  },
} satisfies Partial<Record<SessionTestId, SessionKnownFailure>>;
