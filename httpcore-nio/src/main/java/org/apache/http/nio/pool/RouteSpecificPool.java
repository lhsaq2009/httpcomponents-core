package org.apache.http.nio.pool;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.http.concurrent.BasicFuture;
import org.apache.http.nio.reactor.SessionRequest;
import org.apache.http.pool.PoolEntry;
import org.apache.http.util.Args;
import org.apache.http.util.Asserts;

abstract class RouteSpecificPool<T, C, E extends PoolEntry<T, C>> {

    private final T route;
    private final Set<E> leased;
    private final LinkedList<E> available;
    private final Map<SessionRequest, BasicFuture<E>> pending;

    RouteSpecificPool(final T route) {
        super();
        this.route = route;
        this.leased = new HashSet<E>();
        this.available = new LinkedList<E>();
        this.pending = new HashMap<SessionRequest, BasicFuture<E>>();
    }

    public T getRoute() {
        return this.route;
    }

    protected abstract E createEntry(T route, C conn);

    public int getLeasedCount() {
        return this.leased.size();
    }

    public int getPendingCount() {
        return this.pending.size();
    }

    public int getAvailableCount() {
        return this.available.size();
    }

    public int getAllocatedCount() {
        return this.available.size() + this.leased.size() + this.pending.size();
    }

    public E getFree(final Object state) {
        if (!this.available.isEmpty()) {
            if (state != null) {
                final Iterator<E> it = this.available.iterator();
                while (it.hasNext()) {
                    final E entry = it.next();
                    if (state.equals(entry.getState())) {
                        it.remove();
                        this.leased.add(entry);
                        return entry;
                    }
                }
            }
            final Iterator<E> it = this.available.iterator();
            while (it.hasNext()) {
                final E entry = it.next();
                if (entry.getState() == null) {
                    it.remove();
                    this.leased.add(entry);
                    return entry;
                }
            }
        }
        return null;
    }

    public E getLastUsed() {
        return this.available.isEmpty() ? null : this.available.getLast();
    }

    public boolean remove(final E entry) {
        Args.notNull(entry, "Pool entry");
        if (!this.available.remove(entry)) {
            if (!this.leased.remove(entry)) {
                return false;
            }
        }
        return true;
    }

    public void free(final E entry, final boolean reusable) {
        Args.notNull(entry, "Pool entry");
        final boolean found = this.leased.remove(entry);
        Asserts.check(found, "Entry %s has not been leased from this pool", entry);
        if (reusable) {
            this.available.addFirst(entry);
        }
    }

    public void addPending(final SessionRequest request, final BasicFuture<E> future) {
        this.pending.put(request, future);
    }

    private BasicFuture<E> removeRequest(final SessionRequest request) {
        return this.pending.remove(request);
    }

    public E createEntry(final SessionRequest request, final C conn) {
        final E entry = createEntry(this.route, conn);
        this.leased.add(entry);
        return entry;
    }

    public boolean completed(final SessionRequest request, final E entry) {
        final BasicFuture<E> future = removeRequest(request);
        if (future != null) {
            return future.completed(entry);
        }
        request.cancel();
        return false;
    }

    public void cancelled(final SessionRequest request) {
        final BasicFuture<E> future = removeRequest(request);
        if (future != null) {
            future.cancel(true);
        }
    }

    public void failed(final SessionRequest request, final Exception ex) {
        final BasicFuture<E> future = removeRequest(request);
        if (future != null) {
            future.failed(ex);
        }
    }

    public void timeout(final SessionRequest request) {
        final BasicFuture<E> future = removeRequest(request);
        if (future != null) {
            future.failed(new ConnectException("Timeout connecting to [" + request.getRemoteAddress() + "]"));
        }
    }

    public void shutdown() {
        for (final SessionRequest request: this.pending.keySet()) {
            request.cancel();
        }
        this.pending.clear();
        for (final E entry: this.available) {
            entry.close();
        }
        this.available.clear();
        for (final E entry: this.leased) {
            entry.close();
        }
        this.leased.clear();
    }

    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append("[route: ");
        buffer.append(this.route);
        buffer.append("][leased: ");
        buffer.append(this.leased.size());
        buffer.append("][available: ");
        buffer.append(this.available.size());
        buffer.append("][pending: ");
        buffer.append(this.pending.size());
        buffer.append("]");
        return buffer.toString();
    }

}
