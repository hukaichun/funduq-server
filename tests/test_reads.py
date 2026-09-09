"""Who may read the record — the rule this gateway owns since revision 22.

Core stopped answering it (`funduq#275`): `Funduq.as_reader` and the
`Reader` class are gone, and what is left is `parties_of`, the half only
core can derive because the chains are verified there. The rule itself
lives here now, in `souk_server.reads.may_read`, and these are its tests
— separate from the doors that call it, because there are three of them
and the rule is one.

What it admits did not change when it moved. Revision 21's circle is
still the circle: a thread nobody bound is readable by whoever holds its
id, a bound thread only by its parties.
"""

from __future__ import annotations

import pytest

from funduq import repo
from souk_server.reads import may_read

pytestmark = pytest.mark.anyio


async def test_an_unbound_thread_is_readable_by_whoever_holds_its_id(
    souk, session, register
):
    """The standing capability-by-identifier rule, unchanged. Nothing was
    bound, so nothing named a set of parties, and the id is the whole of
    what there is to hold."""
    served = await register("greeter")
    thread_id = await repo.create_thread(session, served.ref())
    await session.commit()

    assert await may_read(souk, thread_id, None) is True
    assert await may_read(souk, thread_id, "ff" * 32) is True


async def test_a_bound_thread_answers_its_parties_and_nobody_else(
    souk, session, register, new_identity
):
    """The serving provider and the head are parties by construction;
    a key that never touched the thread is not."""
    served = await register("greeter")
    head = new_identity()
    thread_id = await repo.create_thread(
        session, served.ref(), head_key=head.public_key
    )
    await session.commit()

    assert await may_read(souk, thread_id, head.public_key) is True
    assert await may_read(souk, thread_id, served.identity.public_key) is True
    assert await may_read(souk, thread_id, new_identity().public_key) is False
    assert await may_read(souk, thread_id, None) is False


async def test_a_thread_that_does_not_exist_is_not_readable_by_anyone(souk):
    """The trap this whole module exists to keep out of three doors.

    `parties_of` returns `None` for a thread nobody bound *and* for a
    thread that is not there — two answers with one spelling. Revision
    21's `Reader` told them apart, denying the missing one and admitting
    the unbound one. Read the ambiguity the convenient way — `parties is
    None` means "open" — and every id that names nothing becomes readable
    by anyone, which is only one typo away from every id that names
    something bound, and no other test in this repo goes red for it.
    """
    assert await may_read(souk, "thread_that_was_never_created", None) is False
