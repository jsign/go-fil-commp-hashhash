package commp

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"

	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/ipfs/go-car"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	chunker "github.com/ipfs/go-ipfs-chunker"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"
)

var outcar = flag.Bool("outcar", false, "Output generated CAR files")

func TestCommP(t *testing.T) {
	cid, r, err := genCar(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup

	pr, pw := io.Pipe()
	if *outcar {
		wg.Add(1)
		r = io.TeeReader(r, pw)
		go func() {
			defer wg.Done()
			f, err := os.Create(cid.String())
			if err != nil {
				panic(err)
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()
			if _, err = io.Copy(f, pr); err != nil {
				panic(err)
			}
		}()
	}

	cp := NewAccumulator()
	n, err := io.Copy(cp, r)
	if err != nil {
		t.Fatal(err)
	}
	pw.Close()

	rawCommP, paddedSize, err := cp.Digest()
	if err != nil {
		t.Fatal(err)
	}

	commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)

	fmt.Printf(`Finished:
CommP:    %x
CommPCid: %s
Raw bytes:      % 12d bytes
Unpadded piece: % 12d bytes
Padded piece:   % 12d bytes
		`,
		rawCommP,
		commCid,
		n,
		paddedSize/128*127,
		paddedSize,
	)

	wg.Wait()
}

func genCar(seed, size int64) (cid.Cid, io.Reader, error) {
	rs := rand.New(rand.NewSource(seed))

	rf := io.LimitReader(rs, size)
	z := files.NewReaderFile(rf)

	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, nil, err
	}
	prefix.MhType = uint64(mh.BLAKE2B_MIN + 31)

	dserv := dstest.Mock()
	params := ihelper.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   126,
		},
		Dagserv: dserv,
	}

	db, err := params.New(chunker.NewSizeSplitter(z, 1<<20))
	if err != nil {
		return cid.Undef, nil, err
	}
	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, nil, err
	}
	rr, rw := io.Pipe()
	go func() {
		defer rw.Close()
		if err := car.WriteCar(context.Background(), dserv, []cid.Cid{nd.Cid()}, rw); err != nil {
			panic(err) // TODO: fix this
		}
	}()

	return nd.Cid(), rr, nil
}
