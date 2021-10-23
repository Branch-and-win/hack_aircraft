
<h4>Реализованная функциональность</h4>
<ul>
    <li> Алгоритм распределяет ВС по местам стоянки с учетом технологических ограничений с целью минимизации совокупных затрат</li>
    <li> Возможность выбора детализации планирования (любое целочисленное количество минут) </li>
    <li> Возможность остановки алгоритма после определенного количества времени (с получением текущего лучшего решения)
</ul> 
<h4>Особенность проекта в следующем:</h4>
<ul>
 <li>Задача сформулирована как задача линейного целочисленного программирования</li>
 <li>Размерность задачи уменьшена с помощью различных эвристик</li>
 <li>Задача сформулирована с помощью питоновского оптимизационного пакета Pyomo, который позволяет вызывать различные солверы (как коммерческие, так и open-source)</li>  
 </ul>
<h4>Основной стек технологий:</h4>
<ul>
    <li>Python(pandas, pandasql)</li>
	<li>Pyomo</li>
	<li>Cbc (Coin-or branch and cut) solver</li>
 
 </ul>
