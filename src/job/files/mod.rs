// Copyright 2022 Zinc Labs Inc. and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;

pub mod disk;
pub mod memory;

fn get_partition_key_from_filename(filename: &str) -> String {
    let mut pk = filename
        .split('_')
        .filter(|&x| x.contains('='))
        .map(|key| key.replace('.', "_"))
        .join("/");
    pk.push('/');
    pk
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_partition_key_from_filename() {
        let filename = "org/logs/stream_name/2022/10/12/13/";
        let partition_key = get_partition_key_from_filename(filename);
        println!("{}", partition_key);
    }
}
