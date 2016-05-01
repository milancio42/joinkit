# **Joinkit**

Iterator adaptors for efficient SQL-like joins.
The library is documented [here.](http://milancio42.github.io/joinkit)

To use it, put the following code to your Cargo.toml:
```toml
[dependencies]
joinkit = "*"
```
and then include the following code in your crate:
```rust
extern crate joinkit;

use joinkit::Joinkit;
```


----------
## **Binaries**

This crate provides two binaries: `hjoin` and `mjoin`, which can be used
to join data on command line using `Hash Join` and `Merge Join` strategy respectively. 

See the [documentation](http://milancio42.github.io/joinkit) to learn more about the join strategies.
You can also run `hjoin --help` or `mjoin --help` to learn about their usage.

## **Examples**

Prepare test data:
```bash
data_path=/tmp/join
if ! [[ -d $data_path ]]; then mkdir -p $data_path; fi
cd $data_path
gawk 'BEGIN{n=20;for(i=0;i<n;i++){print i ",L"}}' > left-num-20
gawk 'BEGIN{n=20;for(i=(n/2);i<(n+n/2);i++){print i ",R"}}' > right-num-20

gawk 'BEGIN{n=20;for(i=0;i<n;i++){print i ",L"}}' | sort -t , -k 1,1 > left-char-20
gawk 'BEGIN{n=20;for(i=(n/2);i<(n+n/2);i++){print i ",R"}}' | sort -t , -k 1,1 > right-char-20

gawk 'BEGIN{n=1000000;for(i=0;i<n;i++){print i ",L"}}' > left-num-1M
gawk 'BEGIN{n=1000000;for(i=(n/2);i<(n+n/2);i++){print i ",R"}}' > right-num-1M

gawk 'BEGIN{n=1000000;for(i=0;i<n;i++){print i ",L"}}' | sort -t , -k 1,1 > left-char-1M
gawk 'BEGIN{n=1000000;for(i=(n/2);i<(n+n/2);i++){print i ",R"}}' | sort -t , -k 1,1 > right-char-1M
```

clone repository:
```bash
cd ~/some/local/path
git clone https://github.com/milancio42/joinkit.git
cd joinkit
cargo build --release
cd target/release
```

#### **Inner Join**

The output contains only the rows, which have the key present in both input files.  
The join key in the left file is composed by the second and the first column, whereas the join key in the right file is composed by the first and the second column (the order is important).  

**Note**, in case of `hjoin`, the right input data is loaded into `HashMap`.
```bash
./hjoin -1 1 -2 1 $data_path/left-char-20 $data_path/right-char-20

# in order to join on numeric data, use '-u' flag to convert a string to 'u64' (or '-i' to 'i64')
./hjoin -1 1-u -2 1-u $data_path/left-num-20 $data_path/right-num-20
```

This is equivalent to:

```bash
./hjoin -1 1 -2 1 -m inner -R $'\n' -F ',' $data_path/left-char-20 $data_path/right-char-20

./hjoin -1 1 -2 1 --mode inner --in-rec-sep $'\n' --in-field_sep ',' --out-rec-sep $'\n' --out-field-sep ',' $data_path/left-char-20 $data_path/right-char-20

./hjoin -1 1 -2 1 --mode inner --in-rec-sep-left $'\n' --in-rec-sep-right $'\n' --in-field_sep-left ',' --in-field_sep-right ',' --out-rec-sep $'\n' --out-field-sep ',' $data_path/left-char-20 $data_path/right-char-20
```

Since both input files are sorted on the join key, we can get the same results using `mjoin`:

```bash
./mjoin -1 1 -2 1 $data_path/left-char-20 $data_path/right-char-20
```

#### **Left Exclusive Join**

The output contains only the rows, which have the key present in the left
input file exclusively.

```bash
./hjoin -1 1 -2 1 -m left-excl $data_path/left-char-20 $data_path/right-char-20

./mjoin -1 1 -2 1 -m left-excl $data_path/left-char-20 $data_path/right-char-20
```

#### **Left Outer Join**

The output contains the rows, which are union of `inner join` and `left exclusive join`.

```bash
./hjoin -1 1 -2 1 -m left-outer $data_path/left-char-20 $data_path/right-char-20

./mjoin -1 1 -2 1 -m left-outer $data_path/left-char-20 $data_path/right-char-20
```

#### **Right Exclusive Join**

The output contains only the rows, which have the key present in the right
input file exclusively.
Note, in case of `hjoin`, the output is ordered based on `HashMap`'s internal
ordering, which is very likely different from that of the input.
```bash
./hjoin -1 1 -2 1 -m right-excl $data_path/left-char-20 $data_path/right-char-20

./mjoin -1 1 -2 1 -m right-excl $data_path/left-char-20 $data_path/right-char-20
```

#### **Right Outer Join**

The output contains the rows, which are union of `inner join` and `right exclusive join`.
Note, in case of `hjoin`, the output is ordered based on `HashMap`'s internal
ordering, which is very likely different from that of the input.

```bash
./hjoin -1 1 -2 1 -m right-outer $data_path/left-char-20 $data_path/right-char-20

./mjoin -1 1 -2 1 -m right-outer $data_path/left-char-20 $data_path/right-char-20
```

#### **Full Outer Join**

The output contains the rows, which are union of `left exclusive join`, `inner
join` and `right exclusive join`.
Note, in case of `hjoin`, the output is ordered based on `HashMap`'s internal
ordering, which is very likely different from that of the input.

```bash
./hjoin -1 1 -2 1 -m full-outer $data_path/left-char-20 $data_path/right-char-20

./mjoin -1 1 -2 1 -m full-outer $data_path/left-char-20 $data_path/right-char-20
```

## **Performance**
TODO

## **Licence**
Joinkit is licenced under MIT license.

