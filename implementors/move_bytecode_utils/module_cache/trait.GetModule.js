(function() {var implementors = {
"sui_core":[["impl GetModule for <a class=\"struct\" href=\"sui_core/authority/struct.AuthorityStore.html\" title=\"struct sui_core::authority::AuthorityStore\">AuthorityStore</a>"]],
"sui_framework_build":[["impl GetModule for <a class=\"struct\" href=\"sui_framework_build/compiled_package/struct.CompiledPackage.html\" title=\"struct sui_framework_build::compiled_package::CompiledPackage\">CompiledPackage</a>"]],
"sui_storage":[["impl&lt;S: <a class=\"trait\" href=\"sui_types/storage/trait.ObjectStore.html\" title=\"trait sui_types::storage::ObjectStore\">ObjectStore</a>&gt; GetModule for <a class=\"struct\" href=\"sui_storage/package_object_cache/struct.PackageObjectCache.html\" title=\"struct sui_storage::package_object_cache::PackageObjectCache\">PackageObjectCache</a>&lt;S&gt;"]],
"sui_transactional_test_runner":[["impl&lt;'a&gt; GetModule for &amp;'a <a class=\"struct\" href=\"sui_transactional_test_runner/test_adapter/struct.SuiTestAdapter.html\" title=\"struct sui_transactional_test_runner::test_adapter::SuiTestAdapter\">SuiTestAdapter</a>&lt;'_&gt;"]],
"sui_types":[["impl&lt;S: GetModule&lt;Error = <a class=\"enum\" href=\"sui_types/error/enum.SuiError.html\" title=\"enum sui_types::error::SuiError\">SuiError</a>, Item = CompiledModule&gt;&gt; GetModule for <a class=\"struct\" href=\"sui_types/temporary_store/struct.TemporaryStore.html\" title=\"struct sui_types::temporary_store::TemporaryStore\">TemporaryStore</a>&lt;S&gt;"],["impl GetModule for <a class=\"struct\" href=\"sui_types/in_memory_storage/struct.InMemoryStorage.html\" title=\"struct sui_types::in_memory_storage::InMemoryStorage\">InMemoryStorage</a>"],["impl&lt;R&gt; GetModule for <a class=\"struct\" href=\"sui_types/temporary_store/struct.TemporaryModuleResolver.html\" title=\"struct sui_types::temporary_store::TemporaryModuleResolver\">TemporaryModuleResolver</a>&lt;'_, R&gt;<span class=\"where fmt-newline\">where\n    R: GetModule&lt;Item = <a class=\"struct\" href=\"https://doc.rust-lang.org/nightly/alloc/sync/struct.Arc.html\" title=\"struct alloc::sync::Arc\">Arc</a>&lt;CompiledModule&gt;, Error = <a class=\"struct\" href=\"https://docs.rs/anyhow/1.0.69/anyhow/struct.Error.html\" title=\"struct anyhow::Error\">Error</a>&gt;,</span>"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()