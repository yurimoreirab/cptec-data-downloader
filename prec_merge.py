import requests
import os
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup


def show_progress(stage, current, total, extra=''):
    """Mostra uma barra de progresso textual simples e elegante no terminal."""
    bar_width = 28
    stage_width = 28
    if total <= 0:
        total = 1

    if len(stage) > stage_width:
        stage = '...' + stage[-(stage_width - 3):]

    ratio = current / total
    filled = int(bar_width * ratio)
    bar = '#' * filled + '-' * (bar_width - filled)
    pct = ratio * 100
    line = f'\r{stage:<28} [{bar}] {current:>4}/{total:<4} {pct:6.2f}%'
    if extra:
        line += f' | {extra}'
    print(line, end='', flush=True)


def end_progress():
    """Finaliza a linha de progresso atual."""
    print()

def get_directories(url):
    """Extrai os diretórios disponíveis da listagem HTML"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        directories = []
        for link in soup.find_all('a'):
            raw_href = link.get('href')
            if not isinstance(raw_href, str):
                continue
            href = raw_href.strip()
            # Remover trailing slash
            href = href.rstrip('/')
            # Extrair apenas o último componente da URL (o nome do diretório)
            dir_name = href.split('/')[-1] if '/' in href else href
            
            # Filtrar: deve ser números (YYYY, MM, DD) ou estar vazio (pular)
            # Pular links como ../, /, ou não-numéricos
            if dir_name and dir_name != '..' and dir_name.isdigit() and len(dir_name) in [2, 4]:
                if dir_name not in directories:
                    directories.append(dir_name)
        
        return sorted(directories)
    except Exception as e:
        print(f'Erro ao acessar {url}: {e}')
        return []

def get_available_date_range(base_url):
    """Descobre o intervalo de datas disponíveis no site"""
    try:
        # Obter anos disponíveis
        years = get_directories(base_url)
        years = [y for y in years if len(y) == 4 and y.isdigit()]  # Filtrar anos válidos
        if not years:
            print('Nenhum ano encontrado no site')
            return None, None
        
        # Pegar primeiro ano para encontrar start_date
        first_year = years[0]
        first_year_url = urljoin(base_url, f'{first_year}/')
        months = get_directories(first_year_url)
        months = [m for m in months if len(m) == 2 and m.isdigit()]  # Filtrar meses válidos
        if not months:
            print(f'Nenhum mês encontrado no ano {first_year}')
            return None, None
        
        first_month = months[0]
        first_month_url = urljoin(first_year_url, f'{first_month}/')
        days = get_directories(first_month_url)
        days = [d for d in days if len(d) == 2 and d.isdigit()]  # Filtrar dias válidos
        if not days:
            print(f'Nenhum dia encontrado em {first_year}/{first_month}')
            return None, None
        
        first_day = days[0]
        try:
            start_date = datetime.strptime(f'{first_year}-{first_month}-{first_day}', '%Y-%m-%d')
        except ValueError as ve:
            print(f'Data inválida: {first_year}-{first_month}-{first_day}: {ve}')
            return None, None
        
        # Pegar último ano para encontrar end_date
        last_year = years[-1]
        last_year_url = urljoin(base_url, f'{last_year}/')
        months = get_directories(last_year_url)
        months = [m for m in months if len(m) == 2 and m.isdigit()]
        last_month = months[-1]
        last_month_url = urljoin(last_year_url, f'{last_month}/')
        days = get_directories(last_month_url)
        days = [d for d in days if len(d) == 2 and d.isdigit()]
        last_day = days[-1]
        try:
            end_date = datetime.strptime(f'{last_year}-{last_month}-{last_day}', '%Y-%m-%d')
        except ValueError as ve:
            print(f'Data inválida: {last_year}-{last_month}-{last_day}: {ve}')
            return None, None
        
        print(f'Intervalo de dados disponíveis: {start_date.date()} a {end_date.date()}')
        return start_date, end_date
    except Exception as e:
        print(f'Erro ao descobrir intervalo de datas: {e}')
        return None, None

def download_grib_files(start_date=None, end_date=None, local_dir='./downloads'):
    if not os.path.isdir(local_dir):
        os.makedirs(local_dir, exist_ok=True)
    base_url = 'https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/'
    started_at = time.time()
    if isinstance(start_date, str):
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        except ValueError as ve:
            print(f'Data de início inválida: {start_date}: {ve}')
            return
    if isinstance(end_date, str):
        try:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as ve:
            print(f'Data de fim inválida: {end_date}: {ve}')
            return
    # se não houver start_date ou end_date descobrir o intervalo disponível
    if start_date is None or end_date is None:
        print('Descobrindo dados disponíveis no site...')
        discovered_start, discovered_end = get_available_date_range(base_url)
        if discovered_start is None or discovered_end is None:
            print('Não foi possível descobrir o intervalo de datas')
            return
        if start_date is None:
            start_date = discovered_start
        if end_date is None:
            end_date = discovered_end

    total_days = (end_date - start_date).days + 1
    processed_days = 0
    processed_files = 0
    expected_files = (end_date - start_date).days * 24  # Estimativa inicial: 24 arquivos por dia
    downloaded_files = 0
    empty_days = 0
    failed_days = 0

    # Gerar a lista de datas no formato yyyy-mm-dd
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')

        # Formar a URL do diretório para a data atual
        date_url = urljoin(base_url, f'{year}/{month}/{day}/')
        
        try:
            response = requests.get(date_url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                # Coletar links .grib2 válidos
                grib_links = []
                for link in soup.find_all('a'):
                    raw_href = link.get('href')
                    if not isinstance(raw_href, str):
                        continue
                    href = raw_href.strip()
                    if href.endswith('.grib2'):
                        grib_links.append(href)

                # Só criar o diretório se houver arquivos para baixar
                if grib_links:
                    #expected_files += len(grib_links)
                    local_path = os.path.join(local_dir, year, month, day)
                    os.makedirs(local_path, exist_ok=True)

                    # Baixar os arquivos
                    for href in grib_links:
                        file_url = urljoin(date_url, href)

                        # Extrair a hora: os dois últimos dígitos antes de .grib2
                        hour_match = re.search(r'(\d{2})\.grib2$', href)
                        hour = hour_match.group(1) if hour_match else '00'

                        local_file_path = os.path.join(local_path, f'MERGE_{date_str}-T{hour}.grib2')

                        # Fazer download do arquivo
                        file_response = requests.get(file_url, timeout=30, stream=True)
                        file_response.raise_for_status()

                        with open(local_file_path, 'wb') as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)

                        downloaded_files += 1
                        processed_files += 1
                        show_progress(
                            os.path.basename(local_file_path),
                            processed_files,
                            expected_files,
                            extra=(
                                f'data={date_str} dia={processed_days + 1}/{total_days} '
                                f'arquivos={downloaded_files} vazios={empty_days} falhas={failed_days}'
                            )
                        )
                else:
                    empty_days += 1
            else:
                failed_days += 1
        except requests.exceptions.RequestException as e:
            failed_days += 1
            print(f'\nErro ao acessar {date_url}: {e}')

        processed_days += 1
        current_date += timedelta(days=1)

    end_progress()
    elapsed = time.time() - started_at
    return {
        'total_days': total_days,
        'processed_days': processed_days,
        'downloaded_files': downloaded_files,
        'empty_days': empty_days,
        'failed_days': failed_days,
        'elapsed_seconds': elapsed,
    }


def concatenate_grib_files_by_month(local_dir='./MERGE', output_dir=None):
    """Concatena os arquivos GRIB2 baixados em um único arquivo por mês."""
    started_at = time.time()

    if output_dir is None:
        output_dir = os.path.join(local_dir, 'monthly')

    if not os.path.isdir(local_dir):
        print(f'Diretório local não encontrado: {local_dir}')
        return {
            'total_months': 0,
            'concatenated_months': 0,
            'input_files': 0,
            'output_dir': output_dir,
            'elapsed_seconds': 0.0,
        }

    os.makedirs(output_dir, exist_ok=True)

    month_jobs = []

    for year in sorted(os.listdir(local_dir)):
        year_path = os.path.join(local_dir, year)
        if not (os.path.isdir(year_path) and year.isdigit() and len(year) == 4):
            continue

        for month in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month)
            if not (os.path.isdir(month_path) and month.isdigit() and len(month) == 2):
                continue

            monthly_files = []
            for day in sorted(os.listdir(month_path)):
                day_path = os.path.join(month_path, day)
                if not (os.path.isdir(day_path) and day.isdigit() and len(day) == 2):
                    continue

                day_files = [
                    os.path.join(day_path, file_name)
                    for file_name in sorted(os.listdir(day_path))
                    if file_name.endswith('.grib2')
                ]
                monthly_files.extend(day_files)

            if not monthly_files:
                continue

            month_jobs.append((year, month, monthly_files))

    total_months = len(month_jobs)
    concatenated_months = 0
    input_files = 0

    for index, (year, month, monthly_files) in enumerate(month_jobs, start=1):
        output_year_dir = os.path.join(output_dir, year)
        os.makedirs(output_year_dir, exist_ok=True)
        output_file = os.path.join(output_year_dir, f'MERGE_{year}-{month}.grib2')

        with open(output_file, 'wb') as out_f:
            for input_file in monthly_files:
                with open(input_file, 'rb') as in_f:
                    out_f.write(in_f.read())

        concatenated_months += 1
        input_files += len(monthly_files)
        show_progress(
            'Concatenacao',
            index,
            total_months if total_months > 0 else 1,
            extra=f'{year}-{month} arquivos={len(monthly_files)}'
        )

    if total_months > 0:
        end_progress()

    elapsed = time.time() - started_at
    return {
        'total_months': total_months,
        'concatenated_months': concatenated_months,
        'input_files': input_files,
        'output_dir': output_dir,
        'elapsed_seconds': elapsed,
    }


def main(start_date=None, end_date='2010-12-31', local_dir='./MERGE'):
    print('Iniciando download de dados MERGE...')
    download_stats = download_grib_files(
        start_date=start_date,
        end_date=end_date,
        local_dir=os.path.join(local_dir, 'daily'),
    )

    if not download_stats:
        print('Download não concluído.')
        return

    print('Iniciando concatenação mensal...')
    concat_stats = concatenate_grib_files_by_month(local_dir=os.path.join(local_dir, 'daily'),
                                                   output_dir=os.path.join(local_dir, 'monthly'))

    print('\nResumo final')
    print('-' * 60)
    print(
        'Download     : '
        f"{download_stats['processed_days']}/{download_stats['total_days']} dias, "
        f"{download_stats['downloaded_files']} arquivos, "
        f"{download_stats['empty_days']} dias vazios, "
        f"{download_stats['failed_days']} falhas, "
        f"{download_stats['elapsed_seconds']:.1f}s"
    )
    print(
        'Concatenação : '
        f"{concat_stats['concatenated_months']}/{concat_stats['total_months']} meses, "
        f"{concat_stats['input_files']} arquivos de entrada, "
        f"{concat_stats['elapsed_seconds']:.1f}s"
    )
    print(f"Saída mensal  : {concat_stats['output_dir']}")


if __name__ == '__main__':
    main()