import os
import sys
import subprocess
import threading
import queue
import shlex
from pathlib import Path
import re
import time
import argparse

script_path = Path(__file__).parent

parser = argparse.ArgumentParser(description='계정 분산 업로드')
parser.add_argument('source', help='업로드 원본 폴더(예: pikpak:upload)')
parser.add_argument('destination', help='업로드 대상 폴더(예: up:upload >>> up[0-n]:upload로 해석됨)')
parser.add_argument('--move-on-error', help='폴더가 지정되어 있으면 오류 발생 시 해당 폴더로 이동, 없으면 오류 메시지만 출력', default=None)
parser.add_argument('--move-on-retry-fail', help='폴더가 지정되어 있으면 재시도 실패 시 해당 폴더로 이동, 없으면 오류 메시지만 출력', default=None)
parser.add_argument('--by-size', help='이 플래그를 지정하면 크기가 큰 순으로 정렬하여 먼저 전송', action='store_true')    
parser.add_argument('--transfers', help='전송에 사용할 스레드 수, 계정 수는 이보다 크거나 같아야 함', type=int, default=4)
parser.add_argument('--big-speed', help='크기가 big-limit 값보다 큰 파일은 이 속도 이상이 나와야 전송 허용', type=float, default=6.1)
parser.add_argument('--big-limit', help='큰 파일의 기준, 이 크기보다 큰 파일은 big_speed 적용', 
    type=int, default=14*1024*1024*1024)   # default 14GB
parser.add_argument('--allow-transfer', help='이 속도 이상이 되어야 전송 허용', type=float, default=3.5)
parser.add_argument('--allow-first', help='첫 전송 시도의 allow-transfer, 지정 안 하면 allow-transfer', type=float)
parser.add_argument('--allow-after', help='이후 전송 시도의 allow-transfer, 지정 안 하면 allow-transfer', type=float)
parser.add_argument('--retry-limit', help='느린 전송 중지 후 다시 시도하는 횟수', type=int, default=3)
parser.add_argument('--retry-wait', help='느린 전송 중지 후 다시 시도하기 전에 대기하는 시간(초)', type=int, default=3)
parser.add_argument('--failed-wait', help='전송 재시도가 실패한 경우 대기하는 시간', type=int, default=3) # seconds
#parser.add_argument('--upload-limit', help='업로드 폴더 용량이 upload_limit보다 작으면 upload_wait초만큼 대기 후 종료!', type=int, default=50*1024*1024*1024)   # default 50GB
parser.add_argument('--upload-limit', help='업로드 폴더 용량이 upload_limit보다 작으면 upload_wait초만큼 대기 후 종료!', type=int, 
    default=0)   # default 50GB
parser.add_argument('--upload-wait', help='업로드 폴더 용량이 upload_limit보다 작은 경우 이 시간 동안 대기 후 종료!', type=int, 
    default=1800) # seconds
parser.add_argument('--rclone-cmd-options', help='rclone 전송 명령에 추가되는 옵션', 
    default='--multi-thread-streams=1 --transfers=1 -v --drive-chunk-size 128M --retries=1 --stats=10s')
parser.add_argument('--rclone-config', help='rclone 구성 파일 위치', default='/root/.config/rclone/rclone.conf')
parser.add_argument('--debug', help='디버그용 메시지 출력', action='store_true')
group = parser.add_mutually_exclusive_group()
group.add_argument('--include', help='전송 목록 작성시 포함할 폴더')
group.add_argument('--exclude', help='전송 목록 작성시 제외할 폴더')


args = parser.parse_args()
# python3 tptg2.py pikpak:upload up:{1aQl0CKRsO35oOenwL58wOXIzmTwxhL5q} --transfers=6 --big-speed=6.0 --allow-first=6.0 --allow-after=3.0 --retry-limit=6 --retry-wait=3 --move-on-error="pikpak:error" --move-on-retry-fail="pikpak:retry" --rclone-config="/content/rclone/rclone.conf" --rclone-cmd-options="--multi-thread-streams=1 --transfers=1 -v --drive-chunk-size 32M --retries=1 --stats=10s"
######################################
def format_file_size(total_size):
    prefixes = ['B', 'KiB', 'MiB', 'GiB']

    total_size = int(total_size)
    prefix_index = 0
    while total_size >= 1024 and prefix_index < len(prefixes) - 1:
        total_size /= 1024
        prefix_index += 1

    # 소수점 아래 자리수를 제한하여 문자열로 변환
    formatted_size = f'{total_size:.2f} {prefixes[prefix_index]}'

    return formatted_size

"""
def get_file_list(source):
    command = f'rclone lsf "{source}" --recursive --format sp --files-only --config {args.rclone_config}'
    if args.include:
        command += f" --include \"{args.include}\""
    elif args.exclude:
        command += f" --exclude \"{args.exclude}\""
    output = subprocess.check_output(command, shell=True).decode().splitlines()
    if args.by_size:
        output.sort(key=lambda x: int(x.split(";")[0]), reverse=True)
    return output
"""

def get_file_list(source, revert=False):
    print(f'## get_name_mapping - {source=}, {revert=}')
    # 처리용 딕셔너리 얻기
    name_mapping = {}

    cmd = f"rclone lsf \"{source}\" --recursive --format tsp --files-only --config {args.rclone_config} --tpslimit 8 --drive-skip-shortcuts"
    if args.include:
        cmd += f" --include \"{args.include}\""
    elif args.exclude:
        cmd += f" --exclude \"{args.exclude}\""

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    
    if err:
        print(f'오류 발생: {err}')
        return name_mapping
    
    items = [item for item in out.decode().split("\n") if item]
    if args.by_size:
        items.sort(key=lambda x: int(x.split(";")[1]), reverse=True)

    print(f"{source}의 항목 수: {len(items)}개")

    for item in items:
        mtime, size, fname = item.split(';', maxsplit=2)

        if fname in name_mapping:
            print(f'파일 이름 중복: {source}/{fname}')
            continue

        name_mapping[fname] = {
            'mtime': mtime,
            'size': int(size),
            'dirname': os.path.dirname(fname),
            'filename': os.path.basename(fname),
        }

    print(f"name_mapping 생성 완료 - 항목 수: {len(name_mapping)}개")

    return name_mapping

def get_source_stastics(name_mapping):
    each_size = [0] * args.transfers
    #each_files = [{}] * args.transfers
    each_files = []
    for _ in range(args.transfers):
        each_files.append({})

    total_size = 0
    for key, value in name_mapping.items():
        total_size += value['size']
    
    print('='*80)
    print(f'전체 파일 수: {len(name_mapping)}개')
    print(f'전체 크기: {format_file_size(total_size)}')
    print('='*80)
    if total_size < args.upload_limit:
        for i in range(args.upload_wait):
            if i % 10 == 0:
                print(f'업로드할 용량이 작아 대기 후 종료: 남은 시간 {args.upload_wait-i}초')
            time.sleep(1)
        sys.exit()
    
    for key, value in name_mapping.items():
        if value['size'] > args.big_limit:
            each_files[0][key] = value['size']
            each_size[0] += value['size']
        else:
            smallest_slot = each_size.index(min(each_size[1:]))
            each_files[smallest_slot][key] = value['size']
            each_size[smallest_slot] += value['size']

    for index, value in enumerate(each_size):
        print(f'account {index}: {len(each_files[index])}개, {format_file_size(value)}')

    return each_files


def move_file2(thread_id, size, file_to_move, source, destination):
    print(f'{thread_id=}, {size=}, {file_to_move=}, {source=}, {destination=}')
    time.sleep(1)


def move_file(thread_id, size, file_to_move, source, destination):
    remote = destination.split(':')[0]
    
    allow_transfer = args.allow_transfer
    allow_first = args.allow_first if args.allow_first else allow_transfer
    allow_after = args.allow_after if args.allow_after else allow_transfer
    retry_limit = args.retry_limit
    retry_wait = args.retry_wait
    failed_wait = args.failed_wait

    pat_speed_kib = r"\* .+?: .+?, ([0-9\.]+)Ki/s, (.+)"
    #* censored/05-23-23/juta…a-101(6342979744)].mp4: 87% /5.907Gi, 293.015Ki/s, 1h50m37s
    pat_speed_mib = r"\* .+?: .+?, ([0-9\.]+)Mi/s, (.+)"
    #censored/05-23-23/jux-…UX730(3991666688)].iso: 37% /3.718Gi, 5.868Mi/s, 6m48s
    pat_error = r".{19} ERROR : (.+): Not deleting source as copy failed: (.+)"
    #2023/05/23 19:20:22 ERROR : censored/05-23-23/jux-754...詩織(1343257273)].mp4: Not deleting source as copy failed: Post "https://www.go...c7paQPW1g": unexpected EOF
    #2023/05/22 05:03:19 ERROR : 2023-05/Sicario...-HD.MA.TrueHD.7.1.Atmos-FGT.mkv: Not deleting source as copy failed: Post "https://www.go...zFyWyoD7w": net/http: HTTP/1.x transport connection broken: http: ContentLength=268435456 with Body length 21648304
    pat_ended = r".{19} INFO  : (.+): Deleted"
    #2023/05/22 04:56:15 INFO  : sehuatang_107/有毒的欲望:上瘾.Toxic Desire Addiction.2014.720p.avi/有毒的欲望:上瘾.Toxic Desire Addiction.2014.720p.avi: Deleted

    is_bigfile = size > args.big_limit
    on_error_flag = False
    exit_flag = False
    completed = False
    retry_move = 0
    while not (exit_flag or on_error_flag): # 둘 중 하나가 True이면 종료
        if retry_move != 0:
            for _ in range(retry_wait):
                time.sleep(1)
                if _ % 10 == 0:
                    print(f'[T{thread_id}-{retry_move}-000] {file_to_move}({format_file_size(size)}): 대기 중({retry_wait-_}초 남음)')
        exit_flag = True
        retry_move += 1
        turn = 1
        allow_transfer = allow_after if retry_move > 1 else allow_first

        print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): 전송 시작')
        
        cmd = f'rclone moveto "{source}/{file_to_move}" "{remote}:{file_to_move}" --config {args.rclone_config} {args.rclone_cmd_options}'
        #print(f'{cmd=}')
        process = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if process.stderr:
            print(f"[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): rlone 오류: {process.stderr.decode('utf-8')}")

        for line in iter(process.stdout.readline, b''):
            line = line.decode('utf-8').strip()
            #print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): 메시지: {line}')
            match = re.match(pat_error, line)
            if match:
                on_error_flag = True
                continue

            match = re.match(pat_ended, line)
            if match:
                print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): 전송 완료')
                completed = True
                continue
            
            match = re.match(pat_speed_kib, line)
            if match:
                if turn < 5 and turn > 2:
                    print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): {match.group(1).strip()} Ki/s, {match.group(2)}: 느린 속도로 인한 중지')
                    process.terminate()
                    time.sleep(1)
                    if process.poll() is not None:
                        print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): rclone 프로세스 종료됨')
                    else:
                        process.kill()
                        print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): rclone 강제 종료')                

                    turn += 1
                    exit_flag = False
                else:
                    print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): {match.group(1).strip()} Ki/s, {match.group(2)}')
                    turn += 1
                continue

            match = re.match(pat_speed_mib, line)
            if match:
                if (turn < 5 and turn > 2) and ((is_bigfile and float(match.group(1).strip()) < args.big_speed) or (float(match.group(1).strip()) < allow_transfer)):
                    print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): {match.group(1).strip()} Mi/s, {match.group(2)}: 느린 속도로 인한 중지')
                    process.kill()
                    turn += 1
                    exit_flag = False
                else:
                    print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): {match.group(1).strip()} Mi/s, {match.group(2)}')
                    turn += 1
                continue
            if args.debug:
                print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] DEBUG: {line}')
        

        # rclone 프로세스가 종료된 시점, 아래는 이를 확인하기 위한 임시 코드
        if process.poll() is None:
            process.wait()
            if args.debug:
                print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): 확인용 메시지!!! rclone 종료 대기가 필요했다.')
        
        if retry_move > retry_limit:
            exit_flag = True

    
    if on_error_flag and args.move_on_error:
        moveto_cmd = f'rclone moveto "{source}/{file_to_move}" "{args.move_on_error}/{file_to_move}" --config {args.rclone_config}'
        moveto_process = subprocess.Popen(shlex.split(moveto_cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): error 폴더로 이동')
    elif exit_flag and (retry_move > retry_limit) and args.move_on_retry_fail:
        moveto_cmd = f'rclone moveto "{source}/{file_to_move}" "{args.move_on_retry_fail}/{file_to_move}" --config {args.rclone_config}'
        moveto_process = subprocess.Popen(shlex.split(moveto_cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): move-on-retry-fail 폴더로 이동')

        # failed wait
        for _ in range(failed_wait):
            time.sleep(1)
            if _ % 10 == 0:
                print(f'[T{thread_id}-{retry_move}-{str(turn).zfill(3)}] {file_to_move}({format_file_size(size)}): 재시도 실패 후 대기 중({failed_wait-_}초 남음)')
    
    if completed:
        error_occured = False
        cmd = f'rclone moveto "{remote}:{file_to_move}" "{destination}/{file_to_move}" --config {args.rclone_config} --drive-server-side-across-configs'
        try:
            subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError as e:
            print(f"오류 발생: {e}")
            error_occured = True
        
        if error_occured:
            print(f'{source}/{file_to_move} 이동 오류')
        else:
            print(f'{source}/{file_to_move} 업로드 완료')


def process_file_queue(thread_id, each_files, source, destination):
    for key, value in each_files[thread_id].items():
        move_file(thread_id, value, key, source, destination.replace(':', f'{thread_id}:'))


def move_files_in_parallel(source, destination, max_threads=4):
    file_list = get_file_list(source)
    each_files = get_source_stastics(file_list)

    # 스레드 목록
    threads = []

    # 최대 max_threads 개의 스레드로 파일 이동
    for tid in range(max_threads):
        thread = threading.Thread(target=process_file_queue, args=(tid, each_files, source, destination))
        thread.start()
        threads.append(thread)

    # 모든 스레드가 종료될 때까지 대기
    for thread in threads:
        thread.join()

# main
move_files_in_parallel(args.source, args.destination, max_threads=args.transfers)
