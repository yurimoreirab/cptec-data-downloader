import requests
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import xarray as xr
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
import json
from typing import cast
from pandas import DataFrame, Series

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POINTS_FILE = os.path.join(BASE_DIR, 'points.json')
MERGE_BASE_URL = 'https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/'


def carregar_pontos(points_file=POINTS_FILE):
    """Carrega os pontos de interesse a partir do arquivo JSON."""
    try:
        with open(points_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        points_df = DataFrame.from_dict(data.get('ibge_municipios_sedes', []))

        if 'cd_mun' in points_df.columns:
            points_df = points_df.set_index('cd_mun')
        elif 'cod_mun' in points_df.columns:
            points_df = points_df.set_index('cod_mun')

        if 'poi' in points_df.columns:
            points_df = points_df.assign(
                poi=Series(
                    [
                        wkt.loads(value) if isinstance(value, str) else value
                        for value in points_df['poi']
                    ],
                    index=points_df.index,
                    dtype='object',
                )
            )

        return points_df
    except Exception as e:
        print(f'Erro ao carregar pontos em {points_file}: {e}')
        return DataFrame()

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


def download_single_grib_file(target_date, hour, local_dir='./MERGE/daily', overwrite=False):
    """Baixa somente um arquivo MERGE a partir da data e da hora desejadas."""
    base_url = MERGE_BASE_URL

    if isinstance(target_date, str):
        try:
            target_date = datetime.strptime(target_date, '%Y-%m-%d')
        except ValueError as ve:
            print(f'Data inválida: {target_date}: {ve}')
            return None
    elif not isinstance(target_date, datetime):
        print('A data deve ser uma string no formato YYYY-MM-DD ou um datetime.')
        return None

    try:
        hour = f'{int(hour):02d}'
    except (TypeError, ValueError):
        print('A hora deve ser um número inteiro entre 0 e 23.')
        return None

    if not 0 <= int(hour) <= 23:
        print('A hora deve estar entre 0 e 23.')
        return None

    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    day = target_date.strftime('%d')
    date_str = target_date.strftime('%Y-%m-%d')

    date_url = urljoin(base_url, f'{year}/{month}/{day}/')

    try:
        response = requests.get(date_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        selected_href = None
        for link in soup.find_all('a'):
            raw_href = link.get('href')
            if not isinstance(raw_href, str):
                continue

            href = raw_href.strip()
            hour_match = re.search(r'(\d{2})\.grib2$', href)
            if href.endswith('.grib2') and hour_match and hour_match.group(1) == hour:
                selected_href = href
                break

        if selected_href is None:
            print(f'Nenhum arquivo encontrado para {date_str} às {hour}h.')
            return None

        local_path = os.path.join(local_dir, year, month, day)
        os.makedirs(local_path, exist_ok=True)

        local_file_path = os.path.join(local_path, f'MERGE_{date_str}-T{hour}.grib2')
        if os.path.exists(local_file_path) and not overwrite:
            print(f'Arquivo já existe: {local_file_path}')
            return {
                'file_path': local_file_path,
                'file_url': urljoin(date_url, selected_href),
                'downloaded': False,
                'skipped': True,
            }

        file_url = urljoin(date_url, selected_href)
        file_response = requests.get(file_url, timeout=30, stream=True)
        file_response.raise_for_status()

        with open(local_file_path, 'wb') as f:
            for chunk in file_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f'Arquivo baixado com sucesso: {local_file_path}')
        return {
            'file_path': local_file_path,
            'file_url': file_url,
            'downloaded': True,
            'skipped': False,
        }
    except requests.exceptions.RequestException as e:
        print(f'Erro ao baixar o arquivo de {date_str} às {hour}h: {e}')
        return None


def download_grib_files(start_date=None, end_date=None, local_dir='./downloads'):
    if not os.path.isdir(local_dir):
        os.makedirs(local_dir, exist_ok=True)
    base_url = MERGE_BASE_URL
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
    expected_files = total_days * 24  # Estimativa inicial: 24 arquivos por dia
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

def transpond_lon(dataset: xr.Dataset) -> xr.Dataset:
    '''
    Converte um array 0x360 para -180x180, mudando a convenção de coordenadas de longitude.
    Parâmetros:
        - dataset: Um DataArray do xarray representando os dados em formato long.min() lon.max().
    Retorna:
        - Um DataArray do xarray com a longitude no formato -180 a 180.
    '''
    try:
        # Verificar se o buffer tem a forma esperada
        if dataset.longitude.min() < 0:
            print('O buffer já parece estar no formato -180 a 180.')
            return dataset
        else:
            # Transpor o buffer para o formato -180 a 180
            transposed_ds = dataset.copy()
            transposed_ds = dataset.assign_coords(longitude=(dataset.longitude + 180) % 360 - 180)
            return transposed_ds
    except Exception as e:
        print(f'Erro ao transpor longitude: {e}')
        return dataset
    
def projetar_geometrias_para_distancia(gdf: gpd.GeoDataFrame, point: Point) -> tuple[gpd.GeoDataFrame, Point]:
    '''
    Projeta as geometrias para um CRS métrico antes de calcular distâncias.
    '''
    try:
        if gdf.crs is None:
            gdf = gdf.set_crs('EPSG:4326', allow_override=True)

        point_series = gpd.GeoSeries([point], crs=gdf.crs)
        crs = gdf.crs

        if crs is not None and getattr(crs, 'is_geographic', False):
            target_crs = gdf.estimate_utm_crs() or 'EPSG:3857'
            point_projected = cast(Point, point_series.to_crs(target_crs).iloc[0])
            return gdf.to_crs(target_crs), point_projected

        return gdf.copy(), point
    except Exception as e:
        print(f'Erro ao projetar geometrias para cálculo de distância: {e}')
        return gdf.copy(), point


def get_vizinhos_proximos(gdf: gpd.GeoDataFrame, point: Point):
    '''
    Encontra o vizinho mais próximo de um ponto específico em um GeoDataFrame.
    Parâmetros:
        - gdf: Um GeoDataFrame do GeoPandas contendo geometria de pontos.
        - point: Um objeto Point do Shapely representando o ponto de interesse.
    Retorna:
        - O índice do vizinho mais próximo no GeoDataFrame, ou None se o GeoDataFrame estiver vazio.
    '''
    try:
        if gdf.empty:
            print('O GeoDataFrame está vazio. Não é possível encontrar vizinhos.')
            return None

        gdf_projected, point_projected = projetar_geometrias_para_distancia(gdf, point)
        distances = gdf_projected.geometry.distance(point_projected)
        return distances.idxmin()
    except Exception as e:
        print(f'Erro ao encontrar vizinho mais próximo: {e}')
        return None

def regionalizacao_inversa_distancia(gdf: gpd.GeoDataFrame, point: Point, variable_name: str):
    '''
    Realiza a regionalização por inverso da distância para um ponto específico em um GeoDataFrame.
    Parâmetros:
        - gdf: Um GeoDataFrame do GeoPandas contendo geometria de pontos e uma variável de interesse.
        - point: Um objeto Point do Shapely representando o ponto de interesse.
        - variable_name: O nome da variável no GeoDataFrame para a qual a regionalização será aplicada.
    Retorna:
        - O valor regionalizado para o ponto de interesse, ou None se o GeoDataFrame estiver vazio ou se ocorrer um erro.
    '''
    try:
        if gdf.empty:
            print('O GeoDataFrame está vazio. Não é possível realizar regionalização.')
            return None

        gdf_projected, point_projected = projetar_geometrias_para_distancia(gdf, point)
        distances = gdf_projected.geometry.distance(point_projected)

        nearest_indices = distances.nsmallest(4).index
        nearest_neighbors = gdf.loc[nearest_indices].copy()
        nearest_neighbors['distance'] = distances.loc[nearest_indices].clip(lower=1e-12)
        nearest_neighbors['weight'] = 1 / nearest_neighbors['distance']

        weighted_average = (
            (nearest_neighbors[variable_name] * nearest_neighbors['weight']).sum()
            / nearest_neighbors['weight'].sum()
        )

        return weighted_average
    except Exception as e:
        print(f'Erro ao realizar regionalização por inverso da distância: {e}')
        return None
    
def regionalizacao_inversa_distancia_multiplos_pontos(gdf: gpd.GeoDataFrame, points: list[Point], variable_name: str):
    '''
    Realiza a regionalização por inverso da distância para uma lista de pontos específicos em um GeoDataFrame.
    Parâmetros:
        - gdf: Um GeoDataFrame do GeoPandas contendo geometria de pontos e uma variável de interesse.
        - points: Uma lista de objetos Point do Shapely representando os pontos de interesse.
        - variable_name: O nome da variável no GeoDataFrame para a qual a regionalização será aplicada.
    Retorna:
        - Uma lista de valores regionalizados para cada ponto de interesse, ou None se o GeoDataFrame estiver vazio ou se ocorrer um erro.
    '''
    try:
        if gdf.empty:
            print('O GeoDataFrame está vazio. Não é possível realizar regionalização.')
            return None

        regionalized_values = []
        for point in points:
            regionalized_value = regionalizacao_inversa_distancia(gdf, point, variable_name)
            regionalized_values.append(regionalized_value)

        return regionalized_values
    except Exception as e:
        print(f'Erro ao realizar regionalização por inverso da distância para múltiplos pontos: {e}')
        return None
    

def grib_para_geodataframe(grib_file, variable_name='rdp', value_column='precip'):
    """Abre um arquivo GRIB do MERGE e converte a variável desejada para GeoDataFrame."""
    try:
        with xr.open_dataset(grib_file, engine='cfgrib') as dataset:
            if variable_name not in dataset:
                print(f'Variável {variable_name} não encontrada no dataset.')
                return None

            data_array = transpond_lon(dataset)[variable_name]
            df = (
                data_array.to_dataframe(name=value_column)
                .reset_index()
                .dropna(subset=[value_column])
            )

        return gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
            crs='EPSG:4326',
        )
    except Exception as e:
        print(f'Erro ao converter arquivo GRIB para GeoDataFrame: {e}')
        return None


def _calcular_precipitacao_ponto(cod, row, gdf: gpd.GeoDataFrame, value_column='precip'):
    """Calcula a precipitação interpolada para um único ponto."""
    point = row.get('poi')
    if not isinstance(point, Point):
        print(f'Ponto inválido para o código {cod}: {point}')
        return cod, None

    return cod, regionalizacao_inversa_distancia(gdf.copy(), point, value_column)


def calcular_precipitacao_pontos(
    gdf: gpd.GeoDataFrame,
    points_df: DataFrame,
    value_column='precip',
    parallel=False,
    max_workers=None,
):
    """Calcula a precipitação interpolada para todos os pontos cadastrados."""
    if gdf is None or gdf.empty:
        print('GeoDataFrame de precipitação vazio.')
        return {}

    if points_df.empty:
        print('Nenhum ponto de interesse foi carregado.')
        return {}

    precip = {}
    mode = 'paralelo' if parallel else 'série'
    items = list(points_df.iterrows())
    total_points = len(items)
    skipped_points = 0

    print(f'Calculando precipitação em modo {mode} para {total_points} pontos...')

    if parallel:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_calcular_precipitacao_ponto, cod, row, gdf, value_column): cod
                for cod, row in items
            }
            for processed_points, future in enumerate(as_completed(futures), start=1):
                try:
                    cod, value = future.result()
                    if value is not None:
                        precip[cod] = value
                    else:
                        skipped_points += 1
                except Exception as e:
                    cod = futures[future]
                    skipped_points += 1
                    print(f'\nErro ao processar ponto {cod}: {e}')

                show_progress(
                    'Extracao dos valores',
                    processed_points,
                    total_points,
                    extra=f'ultimo={cod} calculados={len(precip)} falhas={skipped_points}'
                )
    else:
        for processed_points, (cod, row) in enumerate(items, start=1):
            cod, value = _calcular_precipitacao_ponto(cod, row, gdf, value_column)
            if value is not None:
                precip[cod] = value
            else:
                skipped_points += 1

            show_progress(
                'Extracao dos valores',
                processed_points,
                total_points,
                extra=f'ultimo={cod} calculados={len(precip)} falhas={skipped_points}'
            )

    end_progress()
    return precip


def exibir_resumo_periodo(download_stats, concat_stats):
    """Exibe um resumo do download por período e da concatenação mensal."""
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


def exibir_resumo_arquivo(download_info, precip, elapsed_points_seconds=None):
    """Exibe um resumo da análise de um único arquivo MERGE."""
    print('\nResumo final')
    print('-' * 60)
    print(f"Arquivo      : {download_info['file_path']}")
    print(f"Fonte        : {download_info['file_url']}")
    print(f"Pontos lidos : {len(precip)}")
    if elapsed_points_seconds is not None:
        print(f"Tempo pontos : {elapsed_points_seconds:.2f}s")


def executar_fluxo_periodo_merge(start_date='2026-02-01', end_date='2026-02-28', local_dir='./MERGE'):
    """Executa o fluxo completo de download por período e concatenação mensal."""
    print('Iniciando download de dados MERGE...')
    download_stats = download_grib_files(
        start_date=start_date,
        end_date=end_date,
        local_dir=os.path.join(local_dir, 'daily'),
    )

    if not download_stats:
        print('Download não concluído.')
        return None

    print('Iniciando concatenação mensal...')
    concat_stats = concatenate_grib_files_by_month(
        local_dir=os.path.join(local_dir, 'daily'),
        output_dir=os.path.join(local_dir, 'monthly'),
    )

    exibir_resumo_periodo(download_stats, concat_stats)
    return {
        'download': download_stats,
        'concat': concat_stats,
    }


def executar_fluxo_arquivo_unico(
    start_date='2026-02-01',
    hour=0,
    local_dir='./MERGE',
    points_file=POINTS_FILE,
    variable_name='tp',
    parallel=True,
    max_workers=10,
):
    """Executa o fluxo de download e análise para um único arquivo MERGE."""
    print('Iniciando download de um arquivo MERGE...')
    download_info = download_single_grib_file(
        target_date=start_date,
        hour=hour,
        local_dir=os.path.join(local_dir, 'daily'),
    )

    if not download_info:
        print('Download não concluído.')
        return None

    print('Convertendo arquivo GRIB para GeoDataFrame...')
    gdf = grib_para_geodataframe(download_info['file_path'], variable_name=variable_name)
    if gdf is None:
        return None

    print('Carregando pontos de interesse...')
    points_df = carregar_pontos(points_file)

    calc_started_at = time.time()
    precip = calcular_precipitacao_pontos(
        gdf,
        points_df,
        value_column='precip',
        parallel=parallel,
        max_workers=max_workers,
    )
    elapsed_points_seconds = time.time() - calc_started_at

    exibir_resumo_arquivo(download_info, precip, elapsed_points_seconds=elapsed_points_seconds)
    #salvar os pontos lidos e a precipitação calculada em um arquivo csv
    output_csv = os.path.join(local_dir, 'precipitation.csv')
    points_df['precipitation'] = points_df.reset_index()['cd_mun'].map(precip).values
    points_df.to_csv(output_csv, index=False)
    print(f"Precipitação salva em: {output_csv}")
    return {
        'download': download_info,
        'points': points_df,
        'precipitation': precip,
        'elapsed_points_seconds': elapsed_points_seconds,
    }


def main_1(start_date='2026-02-01', end_date='2026-02-28', local_dir='./MERGE'):
    """Mantém o fluxo antigo de download por período e concatenação mensal."""
    return executar_fluxo_periodo_merge(start_date=start_date, end_date=end_date, local_dir=local_dir)


def main(
    start_date='2026-02-01',
    hour=0,
    local_dir='./MERGE',
    points_file=POINTS_FILE,
    variable_name='rdp',
    parallel=True,
    max_workers=14,
):
    """Executa o fluxo principal para baixar e analisar um único arquivo MERGE."""
    return executar_fluxo_arquivo_unico(
        start_date=start_date,
        hour=hour,
        local_dir=local_dir,
        points_file=points_file,
        variable_name=variable_name,
        parallel=parallel,
        max_workers=max_workers,
    )


if __name__ == '__main__':
    with open('results.txt', 'w', encoding='utf-8') as f:
        # Redirecionar a saída padrão para o arquivo
        import sys
        sys.stdout = f

        # Executar o fluxo principal
        main()