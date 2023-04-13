// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

import { useRpcClient, useTimeAgo } from '@mysten/core';
import { useQuery } from '@tanstack/react-query';

export function useEpochProgress(suffix: string = 'left') {
    const rpc = useRpcClient();
    const { data } = useQuery(['system', 'state'], () =>
        rpc.getLatestSuiSystemState()
    );

    const start = Number(data?.epochStartTimestampMs ?? 0);
    const duration = Number(data?.epochDurationMs ?? 0);
    const end = start + duration;
    const time = useTimeAgo(end, true, true);
    const progress =
        start && duration
            ? Math.min(((Date.now() - start) / (end - start)) * 100, 100)
            : 0;

    const timeLeftMs = Date.now() - end;
    const timeLeftMin = Math.floor(timeLeftMs / 60000);

    let label;
    if (timeLeftMs >= 0) {
        label = 'Ending soon';
    } else if (timeLeftMin >= -1) {
        label = 'About a min left';
    } else {
        label = `${time} ${suffix}`;
    }

    return {
        epoch: data?.epoch,
        progress,
        label,
    };
}

export function getElapsedTime(start: number, end: number) {
    const diff = end - start;

    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    return {
        hours,
        minutes: minutes - hours * 60,
        seconds: seconds - minutes * 60,
    };
}
