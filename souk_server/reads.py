"""Who may read the record — one rule, in one place, and it is ours now.

Contract revision 22 took reading out of core (`funduq#275`). Until
revision 21 core answered it: `Funduq.as_reader(key)` filtered every read
against the thread's parties, and a door got the rule by passing a key.
Revision 22 deleted that surface. What core keeps is the half only it can
answer — `Funduq.parties_of(thread_id)`, the head, the provider serving
the agent and every key on the thread's runs' chains, because the chains
are there and only there are they verified — and what follows from the
answer belongs to whoever owns the door.

Upstream's argument for the move is worth repeating, because it is also
the argument for this module existing rather than the check being written
out at each door. A denied read used to return `[]` or `None`, which is
what "there is nothing there" also looks like, so a serving layer that
wanted a wider rule was overruled with no signal that anything had been
decided; and the only lever a door held was *which key it passed*, so the
one way to widen was to pass a key already on the chain, which is to
impersonate a party. One rule with two owners is worse than one rule with
one owner, wherever that owner sits.

So this gateway keeps revision 21's rule exactly — nothing about who sees
what changed in this repo when the surface moved — and keeps it here, so
that the next deployment that wants a different rule has one function to
change instead of three doors to find.
"""

from funduq.core import Funduq


async def may_read(funduq: Funduq, thread_id: str, key: str | None) -> bool:
    """Whether `key` may read `thread_id` — revision 21's rule, kept.

    A thread nobody bound is readable by whoever holds its id: the id is
    the capability, which is the standing rule for every unbound thing in
    this system. A bound thread is readable by its parties and by nobody
    else. `None` is nobody, and nobody reads a bound thread.

    **The two shapes of `None` from `parties_of` are not the same answer**,
    which is the whole reason this is a function and not an inlined
    expression. Core returns `None` both for a thread nobody bound *and*
    for a thread that does not exist, and revision 21's `Reader` told them
    apart — a missing thread was denied, an unbound one allowed. Reading
    the ambiguity the convenient way turns "no such thread" into "readable
    by anyone", silently, in a test suite that stays green. Hence the
    `get_thread` first: one extra query, and it buys the distinction the
    tests below assert.
    """
    if await funduq.get_thread(thread_id) is None:
        return False
    parties = await funduq.parties_of(thread_id)
    return parties is None or key in parties
