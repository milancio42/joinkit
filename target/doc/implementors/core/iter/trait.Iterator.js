(function() {var implementors = {};
implementors['joinkit'] = ["impl&lt;L, R, F&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.MergeJoinInner.html' title='joinkit::MergeJoinInner'>MergeJoinInner</a>&lt;L, R, F&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, R: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, F: <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.FnMut.html' title='core::ops::FnMut'>FnMut</a>(&amp;L::Item, &amp;R::Item) -&gt; <a class='enum' href='https://doc.rust-lang.org/nightly/core/cmp/enum.Ordering.html' title='core::cmp::Ordering'>Ordering</a></span>","impl&lt;L, R, F&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.MergeJoinLeftExcl.html' title='joinkit::MergeJoinLeftExcl'>MergeJoinLeftExcl</a>&lt;L, R, F&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, R: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, F: <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.FnMut.html' title='core::ops::FnMut'>FnMut</a>(&amp;L::Item, &amp;R::Item) -&gt; <a class='enum' href='https://doc.rust-lang.org/nightly/core/cmp/enum.Ordering.html' title='core::cmp::Ordering'>Ordering</a></span>","impl&lt;L, R, F&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.MergeJoinLeftOuter.html' title='joinkit::MergeJoinLeftOuter'>MergeJoinLeftOuter</a>&lt;L, R, F&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, R: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, F: <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.FnMut.html' title='core::ops::FnMut'>FnMut</a>(&amp;L::Item, &amp;R::Item) -&gt; <a class='enum' href='https://doc.rust-lang.org/nightly/core/cmp/enum.Ordering.html' title='core::cmp::Ordering'>Ordering</a></span>","impl&lt;L, R, F&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.MergeJoinFullOuter.html' title='joinkit::MergeJoinFullOuter'>MergeJoinFullOuter</a>&lt;L, R, F&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, R: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>, F: <a class='trait' href='https://doc.rust-lang.org/nightly/core/ops/trait.FnMut.html' title='core::ops::FnMut'>FnMut</a>(&amp;L::Item, &amp;R::Item) -&gt; <a class='enum' href='https://doc.rust-lang.org/nightly/core/cmp/enum.Ordering.html' title='core::cmp::Ordering'>Ordering</a></span>","impl&lt;L, K, LV, RV&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.HashJoinInner.html' title='joinkit::HashJoinInner'>HashJoinInner</a>&lt;L, K, RV&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>&lt;Item=<a href='https://doc.rust-lang.org/nightly/std/primitive.tuple.html'>(K, LV)</a>&gt;, K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html' title='core::hash::Hash'>Hash</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a>, RV: <a class='trait' href='https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html' title='core::clone::Clone'>Clone</a></span>","impl&lt;L, K, LV&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.HashJoinLeftExcl.html' title='joinkit::HashJoinLeftExcl'>HashJoinLeftExcl</a>&lt;L, K&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>&lt;Item=<a href='https://doc.rust-lang.org/nightly/std/primitive.tuple.html'>(K, LV)</a>&gt;, K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html' title='core::hash::Hash'>Hash</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a></span>","impl&lt;L, K, LV, RV&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.HashJoinLeftOuter.html' title='joinkit::HashJoinLeftOuter'>HashJoinLeftOuter</a>&lt;L, K, RV&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>&lt;Item=<a href='https://doc.rust-lang.org/nightly/std/primitive.tuple.html'>(K, LV)</a>&gt;, K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html' title='core::hash::Hash'>Hash</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a>, RV: <a class='trait' href='https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html' title='core::clone::Clone'>Clone</a></span>","impl&lt;L, K, LV, RV&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.HashJoinRightExcl.html' title='joinkit::HashJoinRightExcl'>HashJoinRightExcl</a>&lt;L, K, RV&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>&lt;Item=<a href='https://doc.rust-lang.org/nightly/std/primitive.tuple.html'>(K, LV)</a>&gt;, K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html' title='core::hash::Hash'>Hash</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a></span>","impl&lt;L, K, LV, RV&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.HashJoinRightOuter.html' title='joinkit::HashJoinRightOuter'>HashJoinRightOuter</a>&lt;L, K, RV&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>&lt;Item=<a href='https://doc.rust-lang.org/nightly/std/primitive.tuple.html'>(K, LV)</a>&gt;, K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html' title='core::hash::Hash'>Hash</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a>, RV: <a class='trait' href='https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html' title='core::clone::Clone'>Clone</a></span>","impl&lt;L, K, LV, RV&gt; <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a> for <a class='struct' href='joinkit/struct.HashJoinFullOuter.html' title='joinkit::HashJoinFullOuter'>HashJoinFullOuter</a>&lt;L, K, RV&gt; <span class='where'>where L: <a class='trait' href='https://doc.rust-lang.org/nightly/core/iter/trait.Iterator.html' title='core::iter::Iterator'>Iterator</a>&lt;Item=<a href='https://doc.rust-lang.org/nightly/std/primitive.tuple.html'>(K, LV)</a>&gt;, K: <a class='trait' href='https://doc.rust-lang.org/nightly/core/hash/trait.Hash.html' title='core::hash::Hash'>Hash</a> + <a class='trait' href='https://doc.rust-lang.org/nightly/core/cmp/trait.Eq.html' title='core::cmp::Eq'>Eq</a>, RV: <a class='trait' href='https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html' title='core::clone::Clone'>Clone</a></span>",];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        
})()