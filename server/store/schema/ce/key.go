// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ce

import "github.com/vanus-labs/vanus/server/store/block"

type stimeKey struct {
	block.EmptyEntry
	stime int64
}

func (e *stimeKey) GetInt64(ordinal int) int64 {
	if ordinal == StimeOrdinal {
		return e.stime
	}
	return e.EmptyEntry.GetInt64(ordinal)
}

func StimeKey(stime int64) block.Entry {
	return &stimeKey{stime: stime}
}
