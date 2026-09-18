// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The WasmEdge Authors

//===-- wasmedge/runtime/instance/component/stream.h - Stream instance
//-----===//
//
// Part of the WasmEdge Project.
//
//===----------------------------------------------------------------------===//
///
/// \file
/// This file contains the stream instance of the component model.
///
//===----------------------------------------------------------------------===//
#pragma once

#include "common/component_valtype.h"
#include "common/component_variant.h"
#include "runtime/instance/component/function.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace WasmEdge {
namespace Runtime {
namespace Instance {

class ComponentInstance;

namespace Component {

/// The elements of a copy in flight, in guest memory or host storage.
struct StreamBuffer {
  /// The elements the buffer still has room for.
  uint32_t getRemaining() const noexcept { return Length - Progress; }

  Runtime::Component::CanonOptions Opts;
  std::optional<ComponentValType> ElemType;
  const ComponentInstance *ElemTypeInst = nullptr;
  uint64_t Ptr = 0;
  uint32_t Length = 0;
  uint32_t Progress = 0;
  bool IsHost = false;
  std::vector<uint8_t> HostBytes;
  std::vector<ComponentValVariant> HostVals;
  /// The first element of the current copy within HostBytes or HostVals.
  uint32_t HostBase = 0;
};

/// A stream, or a future as a stream carrying exactly one value.
class StreamInstance {
public:
  /// The two roles on a stream.
  enum class Role : uint8_t {
    Reader,
    Writer,
  };

  /// How a copy ended. A stream payload packs `result | (progress << 4)`, a
  /// future one does not.
  enum class CopyResult : uint32_t {
    Completed = 0,
    Dropped = 1,
    Cancelled = 2,
  };

  StreamInstance(bool IsFuture, std::optional<ComponentValType> Elem,
                 const ComponentInstance *Inst) noexcept
      : Future(IsFuture), ElemType(std::move(Elem)), ElemTypeInst(Inst) {}
  StreamInstance(const StreamInstance &) = delete;
  StreamInstance &operator=(const StreamInstance &) = delete;

  /// \name The shape of the stream.
  /// @{
  bool isFuture() const noexcept { return Future; }
  const std::optional<ComponentValType> &getElemType() const noexcept {
    return ElemType;
  }
  /// The instance that created the stream and owns the element type; null
  /// once it is gone.
  const ComponentInstance *getElemTypeInst() const noexcept {
    return ElemTypeInst;
  }
  /// @}

  /// \name The queries on one role.
  /// @{
  /// A copy ended and its result waits to be collected.
  bool isCompleted(Role Role) const noexcept {
    return Status[getIdx(Role)] == CopyState::Completed;
  }
  /// A copy is in flight: blocked, or with its result not yet collected.
  bool isCopying(Role Role) const noexcept {
    const CopyState Cur = Status[getIdx(Role)];
    return Cur == CopyState::Blocked || Cur == CopyState::Completed;
  }
  bool isEnded(Role Role) const noexcept {
    return Status[getIdx(Role)] == CopyState::Ended;
  }
  /// Ended because the peer dropped its handle.
  bool isEndedByPeerDrop(Role Role) const noexcept {
    return EndedByPeerDrop[getIdx(Role)];
  }
  /// Whether Role left its buffer pending for its peer.
  bool isPending(Role Role) const noexcept {
    return PendingRole.has_value() && *PendingRole == Role;
  }
  /// Whether the peer of Role left its buffer pending.
  bool isPeerPending(Role Role) const noexcept {
    return PendingRole.has_value() && *PendingRole != Role;
  }
  /// Whether the peer of Role dropped its handle.
  bool isPeerHandleDropped(Role Role) const noexcept {
    return HandleDropped[getPeerIdx(Role)];
  }
  StreamBuffer &getBuffer(Role Role) noexcept { return Buffer[getIdx(Role)]; }
  const StreamBuffer &getBuffer(Role Role) const noexcept {
    return Buffer[getIdx(Role)];
  }
  const StreamBuffer &getPeerBuffer(Role Role) const noexcept {
    return Buffer[getPeerIdx(Role)];
  }
  /// The waitable set the handle of Role joined; 0 for none.
  uint32_t getSetIdx(Role Role) const noexcept { return SetIdx[getIdx(Role)]; }
  void setSetIdx(Role Role, uint32_t Idx) noexcept {
    SetIdx[getIdx(Role)] = Idx;
  }
  /// The instance whose handle table names the end of Role; null while the
  /// end is in transfer or dropped.
  ComponentInstance *getHolder(Role Role) const noexcept {
    return Holder[getIdx(Role)];
  }
  void setHolder(Role Role, ComponentInstance *Inst) noexcept {
    Holder[getIdx(Role)] = Inst;
  }
  /// Whether a synchronous built-in waits on Role.
  bool isSyncWaiter(Role Role) const noexcept {
    return SyncWaiter[getIdx(Role)];
  }
  void setSyncWaiter(Role Role, bool IsWaiter) noexcept {
    SyncWaiter[getIdx(Role)] = IsWaiter;
  }
  /// @}

  /// \name The queries on the whole stream.
  /// @{
  /// Both handles dropped.
  bool isClosed() const noexcept {
    return HandleDropped[0] && HandleDropped[1];
  }
  /// @}

  /// \name The transitions, driven by the executor.
  /// @{
  /// A copy starts on Role: the number of elements to move now.
  uint32_t onCopy(Role Role) noexcept {
    const size_t Self = getIdx(Role);
    const size_t Peer = getPeerIdx(Role);
    if (HandleDropped[Peer] || CreatorGone) {
      Status[Self] = CopyState::Completed;
      Result[Self] = CopyResult::Dropped;
      return 0;
    }
    // Nobody waits, or the pending buffer is full: this copy blocks.
    if (!isPeerPending(Role) || PendingFull) {
      PendingRole = Role;
      PendingFull = false;
      Status[Self] = CopyState::Blocked;
      return 0;
    }
    // A zero-length copy finishes at once, but a pending zero-length write
    // yields to any read.
    if (Buffer[Self].Length == 0 && !(Role == StreamInstance::Role::Reader &&
                                      Buffer[Peer].getRemaining() == 0)) {
      Status[Self] = CopyState::Completed;
      Result[Self] = CopyResult::Completed;
      return 0;
    }
    // A pending zero-length peer only probed: it finishes, and this copy
    // becomes the pending one.
    if (Buffer[Peer].getRemaining() == 0) {
      Status[Peer] = CopyState::Completed;
      Result[Peer] = CopyResult::Completed;
      PendingRole = Role;
      PendingFull = false;
      Status[Self] = CopyState::Blocked;
      return 0;
    }
    ActiveRole = Role;
    return std::min(Buffer[Self].getRemaining(), Buffer[Peer].getRemaining());
  }

  /// Count elements moved; the pending buffer stays while it has room.
  void onMove(uint32_t Count) noexcept {
    if (!ActiveRole.has_value() || !PendingRole.has_value()) {
      return;
    }
    const size_t Active = getIdx(*ActiveRole);
    const size_t Pending = getIdx(*PendingRole);
    Buffer[Active].Progress += Count;
    Buffer[Pending].Progress += Count;
    Status[Active] = CopyState::Completed;
    Result[Active] = CopyResult::Completed;
    Status[Pending] = CopyState::Completed;
    Result[Pending] = CopyResult::Completed;
    if (Buffer[Pending].getRemaining() == 0) {
      PendingFull = true;
    }
    ActiveRole.reset();
  }

  /// Take the result and progress of the finished copy on Role.
  std::pair<CopyResult, uint32_t> onCollect(Role Role) noexcept {
    const size_t Self = getIdx(Role);
    const CopyResult Taken = Result[Self];
    const uint32_t Count = Buffer[Self].Progress;
    resetPending(Role);
    if (Taken == CopyResult::Dropped ||
        (Future && Taken == CopyResult::Completed)) {
      Status[Self] = CopyState::Ended;
      EndedByPeerDrop[Self] = Taken == CopyResult::Dropped;
    } else {
      Status[Self] = CopyState::Idle;
    }
    return {Taken, Count};
  }

  /// Cancel the copy on Role.
  void onCancel(Role Role) noexcept {
    const size_t Self = getIdx(Role);
    if (Status[Self] == CopyState::Blocked) {
      resetPending(Role);
      Status[Self] = CopyState::Completed;
      Result[Self] = CopyResult::Cancelled;
    } else if (Status[Self] == CopyState::Completed &&
               Result[Self] == CopyResult::Completed) {
      resetPending(Role);
      if (!Future) {
        Result[Self] = CopyResult::Cancelled;
      }
    }
  }

  /// The creating instance goes: a blocked copy ends as dropped, and so does
  /// every later one. The handles others still hold stay valid.
  void onCreatorGone() noexcept {
    CreatorGone = true;
    ElemTypeInst = nullptr;
    if (PendingRole.has_value()) {
      const size_t Pending = getIdx(*PendingRole);
      PendingRole.reset();
      PendingFull = false;
      Status[Pending] = CopyState::Completed;
      Result[Pending] = CopyResult::Dropped;
    }
  }

  /// Drop the handle of Role.
  void onDrop(Role Role) noexcept {
    const size_t Self = getIdx(Role);
    HandleDropped[Self] = true;
    Holder[Self] = nullptr;
    resetPending(Role);
    Status[Self] = CopyState::Ended;
    if (isPeerPending(Role) && (!Future || !PendingFull)) {
      PendingRole.reset();
      PendingFull = false;
      const size_t Peer = getPeerIdx(Role);
      Status[Peer] = CopyState::Completed;
      Result[Peer] = CopyResult::Dropped;
    }
  }
  /// @}

private:
  /// The copy state machine of one role.
  enum class CopyState : uint8_t {
    Idle,
    Blocked,
    Completed,
    Ended,
  };

  size_t getIdx(Role Role) const noexcept { return static_cast<size_t>(Role); }
  size_t getPeerIdx(Role Role) const noexcept {
    return 1 - static_cast<size_t>(Role);
  }
  /// Role gives the pending buffer up if it is the pending one.
  void resetPending(Role Role) noexcept {
    if (isPending(Role)) {
      PendingRole.reset();
      PendingFull = false;
    }
  }

  /// \name Data of stream.
  /// @{
  const bool Future;
  const std::optional<ComponentValType> ElemType;
  const ComponentInstance *ElemTypeInst;
  /// The role that arrived first and waits for its peer.
  std::optional<Role> PendingRole;
  /// The pending buffer is full: no new copy joins it.
  bool PendingFull = false;
  /// The creating instance is gone: nothing moves any more.
  bool CreatorGone = false;
  /// The role whose copy met the pending buffer.
  std::optional<Role> ActiveRole;
  /// The state of each role, indexed by Role.
  CopyState Status[2] = {CopyState::Idle, CopyState::Idle};
  /// The result of the copy while Completed.
  CopyResult Result[2] = {CopyResult::Completed, CopyResult::Completed};
  StreamBuffer Buffer[2];
  bool HandleDropped[2] = {false, false};
  ComponentInstance *Holder[2] = {nullptr, nullptr};
  bool EndedByPeerDrop[2] = {false, false};
  uint32_t SetIdx[2] = {0, 0};
  bool SyncWaiter[2] = {false, false};
  /// @}
};

} // namespace Component
} // namespace Instance
} // namespace Runtime
} // namespace WasmEdge
