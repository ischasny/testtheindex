package main

import (
	"fmt"
	"net/http"

	adminserver "github.com/filecoin-project/index-provider/server/admin/http"
	"github.com/urfave/cli/v2"
)

var RandomAdCmd = &cli.Command{
	Name:   "randomAd",
	Usage:  "Generate and publish a random Ad",
	Flags:  randomAdFlags,
	Action: randomAdCommand,
}

func randomAdCommand(cctx *cli.Context) error {
	req := adminserver.RandomAdReq{
		XpCount:   randomAdXpCountValue,
		MhsCount:  randomAdMhsCountValue,
		ContextID: randomAdContextIDValue,
		Override:  randomAdOverrideValue,
	}

	resp, err := doHttpPostReq(cctx.Context, adminAPIFlagValue+"/admin/randomAd", req)

	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errFromHttpResp(resp)
	}

	var res adminserver.RandomAdRes
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("received ok response from server but cannot decode response body. %v", err)
	}
	msg := fmt.Sprintf("Announced a new random advertisement: %s, multihashes: %v, cids: %v\n", res.AdvId, res.Mhs, res.Cids)

	_, err = cctx.App.Writer.Write([]byte(msg))
	return err
}
