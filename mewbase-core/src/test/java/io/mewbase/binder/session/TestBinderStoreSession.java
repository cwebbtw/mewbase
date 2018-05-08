package io.mewbase.binder.session;

import io.mewbase.binders.BinderStore;

import java.util.function.Supplier;

public interface TestBinderStoreSession extends AutoCloseable, Supplier<BinderStore> {}