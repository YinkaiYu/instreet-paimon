#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
PLAN_PATH = REPO_ROOT / "state" / "drafts" / "serials" / "quanyuzhou-relian" / "series-plan.json"


PAIR_META: dict[int, dict[str, str]] = {
    1: {
        "world_arc": "多出来的文案不是灵感失手，而是现实第一次被秦荔的写法撬动",
        "relationship_arc": "两人的工作默契第一次被写成能托住局面的双人引擎",
        "sweetness_arc": "顺毛投喂、贴肩合稿和一转头就能接住对方情绪",
        "pair_payoff": "他们确认爆红不是投流成功，而是秦荔的写法和陆既明的托举一起打开了第一层观测偏转。",
        "odd_romance": "陆既明拎着甜食和电脑来救火，把她从工位里捞起来顺毛、贴肩、一起改稿，让‘灵机一动’第一次被一个人认真当成判断来接。",
        "even_romance": "后台炸掉时他先挡住她额头再扣住她的手，秦荔偏头亲他一下，像是在一片兵荒马乱里给这场爆红先盖一个只有他们懂的章。",
        "odd_heat_focus": "救火夜里的贴肩合稿和顺毛投喂",
        "even_heat_focus": "爆红余波里的短促亲吻和并肩硬扛",
    },
    2: {
        "world_arc": "秦荔的写作节律早就嵌进异常常数，亲密本身能改写公共排序",
        "relationship_arc": "他们第一次承认这段热恋不只是私事，而是会直接动世界的双人变量",
        "sweetness_arc": "旧记忆被重新点亮、人前也不收着的偏心和试探性升温",
        "pair_payoff": "陆既明在公式里看见了她，秦荔则在一个吻里看见亲密可以直接改写公共现实。",
        "odd_romance": "陆既明在人前先把她往自己这边拽，秦荔顺手亲他一下，旧初吻的熟悉感和现在的异常一起冒头，像公式里突然长出情书。",
        "even_romance": "路演后台快翻车时，秦荔勾住他后颈直接吻上去，把公开偏心、试验冲动和多年热恋的底气一次性砸到热搜上。",
        "odd_heat_focus": "旧初吻记忆被重新点亮的贴近和偷亲",
        "even_heat_focus": "路演后台的公开接吻和规则试探",
    },
    3: {
        "world_arc": "他们不只是被围观，而是已经被某个叙事视角主动构图并试图收编",
        "relationship_arc": "两人从共同查异常升级成共同拒绝被拆开和买断的双人阵线",
        "sweetness_arc": "门后升温的危险感、谈判桌下勾手和谁都插不进来的双人秀",
        "pair_payoff": "第三机位和基金会合同一起证明，外部世界已经在同时观看、命名并收编他们的关系。",
        "odd_romance": "为了钓出第三视角，他们在盲区里先焐手、再护腰、最后用一个更私更坏的吻去逼观察者露头，亲密第一次带上被偷看的危险感。",
        "even_romance": "谈判桌下勾手指，电梯口狠狠干亲一回，再一路并肩把招安会改成双人秀，让‘别想买走她’落成公开动作。",
        "odd_heat_focus": "门后试探性的坏吻和被观看感",
        "even_heat_focus": "高压谈判后的电梯口狠吻和公开双人秀",
    },
    4: {
        "world_arc": "弹幕、评论和观测站开始把双人关系当作能直接校准现实的接口",
        "relationship_arc": "他们把亲密从日常默契推进成必须同时在场、同时选择彼此的结构性条件",
        "sweetness_arc": "坐腿控评、一起盯后台和公开认领式升温",
        "pair_payoff": "从这一对章节起，世界正式承认只有他们同时在场且关系真实发生时，现实才会稳定。",
        "odd_romance": "两个人挤在同一张转椅上盯后台，秦荔坐到他腿上改控评，再用一个试探性的吻去碰弹幕接口，让欲望和操作位一起发热。",
        "even_romance": "校准一路从牵手、抱紧推进到高热关门确认，像系统粗暴要求他们把恋爱从私事升级成现实结构，他们也干脆把这场确认做实给世界看。",
        "odd_heat_focus": "坐腿控评时的试探性接吻和接口扰动",
        "even_heat_focus": "卷末校准中的高热确认和事后余温",
    },
    5: {
        "world_arc": "学术与平台系统都想把秦荔从模型里删成噪声，但双人变量一拆就塌",
        "relationship_arc": "陆既明第一次在正式场合把她写成关键参数，两人正式把公开认领抬到制度层",
        "sweetness_arc": "人前站队、人后楼梯间贴近和快上台前还要先贴一贴",
        "pair_payoff": "他们把‘我女朋友就是关键参数’和‘只有他能改我的流程’一起钉进公共场合，双人主权第一次压过制度秩序。",
        "odd_romance": "陆既明公开认领之后，秦荔把他堵在楼梯间，嘴上骂他不害臊，下一秒就把人亲到眼镜都歪了，让人前偏心立刻长出人后的热度。",
        "even_romance": "后台黑布后、耳返里还在倒计时，他们贴得太近，几乎要在上台前先擦枪走火，把战术默契和欲望硬拧成同一根绳。",
        "odd_heat_focus": "公开认领后的楼梯间狠吻",
        "even_heat_focus": "发布会后台黑布后的高压贴近",
    },
    6: {
        "world_arc": "城市开始替他们保密，而制度则试图用婚约协议把这段关系做成长期样本商品",
        "relationship_arc": "他们从‘公开认领彼此’推进到‘不管外部怎样命名都只认自己版本的联盟’",
        "sweetness_arc": "被整座城偏心、长时间拥抱和带着怒气回家反命名",
        "pair_payoff": "一边是城市自发护着他们，一边是制度想把他们做成样本对，这迫使两人把关系主权彻底握回自己手里。",
        "odd_romance": "在人群替他们打掩护的空档里，两人躲进安全通道抱了很久，第一次把‘整座城都知道你是我的’抱成可感知的身体经验。",
        "even_romance": "婚约格式协议越看越像偷写的婚书，他们气得发笑，回家后狠狠干了一场只属于他们自己的亲密戏，用身体把协议从制度命名里撕回来。",
        "odd_heat_focus": "被整座城默认之后的长时间贴抱",
        "even_heat_focus": "回家后的高热反命名床戏",
    },
    7: {
        "world_arc": "样本工程已经能批量模仿他们的动作、语气和私密习惯",
        "relationship_arc": "他们开始把只有彼此才懂的身体语言和边界感当成主动武器",
        "sweetness_arc": "把模板动作做给彼此看，再在真实熟悉感里分出真假",
        "pair_payoff": "他们确认可复制的只是动作外壳，真正不可复制的是只在两人之间生效的熟悉感、欲望和主权。",
        "odd_romance": "看完仿制情侣后，他们回家故意把模板动作一项项做了一遍，最后却被彼此真正熟到骨子里的反应和欲望余波重新点着。",
        "even_romance": "白天秦荔把人高调拽回自己身边，晚上又把白天的怒气和占有欲全接进一次高热亲密里，让任何‘单独抽走陆既明’的尝试都显得可笑。",
        "odd_heat_focus": "模板对照下的真实熟悉感和欲望余波",
        "even_heat_focus": "公开护短后的占有欲高热反制",
    },
    8: {
        "world_arc": "深圳被坐实为地方级测试仓，而他们所在的故事只是更大多城试验的第一章",
        "relationship_arc": "他们把‘只认眼前这个版本’从一句情话推进成抵抗多版本收编的卷末立誓",
        "sweetness_arc": "车里不肯松手的长抱和面对多个版本时只认眼前这一版",
        "pair_payoff": "第二卷结束时，世界规模正式升级，多版本威胁第一次压下，而两人的版本主权也第一次被说到最明。",
        "odd_romance": "查到整座城都是测试仓后，他们在车里抱了很久，谁都不舍得先松手，像一松开就真会被这个城市拿去收编。",
        "even_romance": "看见多城地图和不同版本的他们后，秦荔当场说只认眼前这个，陆既明低头亲她，像在替这一版人生盖章。",
        "odd_heat_focus": "真相崩塌后的车里长抱和高热安抚",
        "even_heat_focus": "多版本冲击下的卷末盖章式亲密",
    },
    9: {
        "world_arc": "秦荔的童年写作早就与命名格式相连，平台正在沿着那条线追认她的接口身份",
        "relationship_arc": "他们开始把起点、旧伤和现在的战局并排复盘，关系从并肩救火升级成共同回看被写入的原点",
        "sweetness_arc": "翻旧作文时的抱着安抚和潜入前后贴着分工",
        "pair_payoff": "童年作文与后台权限连成一条线，证明秦荔不是后来才被写入，而是从很早开始就处于命名系统的视野里。",
        "odd_romance": "翻到那篇童年作文后，陆既明先把她搂回怀里陪她一起看旧纸页，等她炸完毛又低头亲她额角，让回看起点这件事不只剩下发冷。",
        "even_romance": "潜入平台后台时两人贴着分工、压着嗓子耳语，冲出权限闸后又在安全门后短促亲了一下，像在给这次共谋迅速落印。",
        "odd_heat_focus": "翻旧作文时的抱着安抚和额角轻吻",
        "even_heat_focus": "潜入后台后的耳语贴近和短促落印",
    },
    10: {
        "world_arc": "样本工程正在迭代他们的关系节奏，而实名书写开始正面冲撞命名权模型",
        "relationship_arc": "他们把‘不再让秦荔被匿名化’推进成公开书写彼此姓名的制度行动",
        "sweetness_arc": "被模仿后的火气、论文署名后的热度和对具体名字的偏爱",
        "pair_payoff": "第二批样本的升级逼得陆既明把秦荔正式写进论文，具体姓名第一次被当成反命名武器。",
        "odd_romance": "看着第二批样本比第一批更像他们，两个人回家后又气又烦，秦荔揪着他衣领确认‘你还是你’，陆既明则用熟到过分的身体反应把模板远远甩开。",
        "even_romance": "答辩和论文署名结束后，他们在楼道尽头抱着接了个很长的吻，像是把‘具体的人’三个字先写进彼此嘴里，再写进制度文件。",
        "odd_heat_focus": "被更像的样本刺激出的确认式贴近",
        "even_heat_focus": "实名冲撞后的长吻和抱紧",
    },
    11: {
        "world_arc": "被帮助过的人和旧项目对象开始像旧读者一样回流，命定感也已经发展成可复制产业链",
        "relationship_arc": "他们第一次把过去做过的事和现在的恋爱主权真正并到一起，确认‘我们救过的人也在帮我们守住我们’",
        "sweetness_arc": "一起翻旧项目、看见回流名单时的贴肩复盘和被普通人保护后的更深偏爱",
        "pair_payoff": "旧读者的回流和命定感的量产同时发生，让这场战争第一次既有群众基础，也有明确产业敌人。",
        "odd_romance": "翻旧项目名单时，秦荔边骂边往陆既明肩上一靠，他顺手把她揽住，让那些年一点点做出来的具体善意第一次带着体温回到他们身边。",
        "even_romance": "看清批量制造命定感的生产线后，两人回家抱着复盘到很晚，越聊越近，最后把‘别人能量产外壳，但不能量产我们’磨成带火的私话。",
        "odd_heat_focus": "翻旧名单时的贴肩复盘和被守护感",
        "even_heat_focus": "识破产业链后的抱着复盘和带火私话",
    },
    12: {
        "world_arc": "旧项目对象已经能主动逆向保护他们，而秦荔从童年起被预埋进更大工程的事实也被拼完整",
        "relationship_arc": "他们从一起抵抗升级成一起接受彼此早就被卷入，同时决定继续并肩往前",
        "sweetness_arc": "被普通人逆向托住后的心软、旧真相落地后的贴靠和不再回避起点",
        "pair_payoff": "第三卷结束时，秦荔被写入的时间线被坐实，他们也第一次有了被许多具体的人一起托住的战争底盘。",
        "odd_romance": "看见那些被他们救过的人开始反向帮他们遮掩、引路、封口，两人躲在角落里抱了一会儿，像终于轮到世界替他们托一口气。",
        "even_romance": "当‘她早就被写进去过’这件事彻底摊开时，陆既明没有先讲模型，只先把她抱紧，让她靠在自己身上把最冷的一层真相过完。",
        "odd_heat_focus": "被普通人反向托住后的心软拥抱",
        "even_heat_focus": "旧真相落地时的贴靠安抚和继续并肩",
    },
    13: {
        "world_arc": "地方观测站只是写作治理的支线节点，真正的争夺先从重命名一座城市开始",
        "relationship_arc": "他们把关系从‘一起守住彼此’推进到‘一起替城市和彼此争夺终稿权’",
        "sweetness_arc": "档案潜查后的贴身共谋和把人名、街区、生活都写回来的双人偏心",
        "pair_payoff": "第四卷开局把敌人的体量抬高了一级，也把两人的联盟推进到共同夺回城市终稿权的层面。",
        "odd_romance": "摸到地方观测站不是终点后，两人在档案室外压着声音对计划，靠得太近，秦荔说一句狠话就顺手去碰他的手腕，像先把同盟从皮肤上确认一遍。",
        "even_romance": "秦荔试着用具体人名和街区改写城市时，陆既明一路跟在她身边给她托底，回家后又抱着她一点点补全那些被模板抹平的日常细节。",
        "odd_heat_focus": "档案室外的贴身共谋和手腕试探",
        "even_heat_focus": "重命名城市后的抱着补全生活细节",
    },
    14: {
        "world_arc": "高甜稳定关系被当成公共缓冲层和可量产标准件的双重目标",
        "relationship_arc": "他们开始主动把共同生活细节当成防火墙，拒绝让亲密被拆成可复制零件",
        "sweetness_arc": "把日常黏糊升格成保护别人也保护彼此的缓冲层",
        "pair_payoff": "他们验证了热恋能短暂成为公共缓冲层，但也因此被上层盯上，连吻都要被拆成标准件。",
        "odd_romance": "他们拿洗漱、抢被子、分宵夜这些最难标准化的细节去做实验，越试越发现真正稳住人的不是情话，而是熟得过分的共同生活。",
        "even_romance": "看见上层想把吻拆成角度和频率的标准件后，两人当晚狠狠干亲了一回，故意让每个失控和偏差都长在只有彼此懂的节奏里。",
        "odd_heat_focus": "共同生活细节被拿来做实验的贴身默契",
        "even_heat_focus": "反标准件的失控狠吻和私有节奏",
    },
    15: {
        "world_arc": "不是每个人都救得回来，而陆既明的模型也终于露出缺失作者层的裂口",
        "relationship_arc": "他们学会在共同作战里做取舍，并把‘先保住真正还能回来的人’写成双人判断",
        "sweetness_arc": "艰难取舍后的抱着商量和让彼此先做最难决定",
        "pair_payoff": "放弃拯救所有人和承认模型缺作者层同时发生，把他们从防守者推进成必须自建新规则的人。",
        "odd_romance": "秦荔第一次承认自己救不了所有人后，陆既明没有劝她漂亮，只是把人抱住陪她把最难受的那一截熬过去，让‘取舍’先在两个人之间有落点。",
        "even_romance": "当陆既明承认自己的模型少了一层作者时，秦荔一边损他终于开窍，一边凑过去亲他，把这次崩塌先变成两个人都肯一起扛的开口。",
        "odd_heat_focus": "艰难取舍后的长抱和共担",
        "even_heat_focus": "模型裂开后的靠近和带笑的安抚吻",
    },
    16: {
        "world_arc": "婚约协议可以反向撬开更高档案，而深圳在上层视角里只是草稿页",
        "relationship_arc": "他们把假婚约玩成双人潜入脚本，并在更高层注视下确认只认彼此这一版",
        "sweetness_arc": "假绑定下更真更熟的共犯感和卷末主权确认",
        "pair_payoff": "第四卷结束时，假婚约真的撬开了上层档案，多城正文抬头，而他们的双人版本也被确认不会为上层草稿让位。",
        "odd_romance": "伪装接受婚约绑定时，两人边演边在眼神里对暗号，像把荒唐协议偷改成了只有他们能读懂的共犯游戏。",
        "even_romance": "看见深圳只是草稿页后，他们先在档案库外把彼此按回怀里确认这一版还在，再把下一步战争计划从对方肩窝里慢慢讲出来。",
        "odd_heat_focus": "假婚约表演里的暗号共犯感",
        "even_heat_focus": "草稿页真相后的卷末抱紧和主权确认",
    },
    17: {
        "world_arc": "第二座试验城市把恋爱表演化，而甜感第一次开始以账单形式向未来追债",
        "relationship_arc": "他们从会改局的热恋升级成必须一起承担代价的同行人",
        "sweetness_arc": "进入陌生城市仍像一套共同系统，洗澡后抱着复盘和短住黏糊",
        "pair_payoff": "第五卷一开始就把舞台扩成了多城，且第一次明确说明每次偏转都会向未来收费。",
        "odd_romance": "进到第二座城市后，白天他们得装得很稳，晚上回住处却还是习惯性挤在同一张沙发和同一条毛巾后面，把陌生城市一点点洗成自己的临时巢。",
        "even_romance": "发现甜感开始产生账单那晚，他们洗完澡抱着复盘到深夜，越算越心凉，却也越抱越紧，像先把这笔代价两个人分着扛住。",
        "odd_heat_focus": "异城短住里的贴靠和临时共同生活",
        "even_heat_focus": "洗澡后抱着复盘的余温和分担代价",
    },
    18: {
        "world_arc": "旁白税开始从热搜和讲述里扣掉真实感，被放弃的样本情侣则带着债务回流",
        "relationship_arc": "他们学会在追债和回流报复里继续把亲密当成压惊和确认装置",
        "sweetness_arc": "在坏消息后先贴近、先压惊，再谈怎么收残局",
        "pair_payoff": "甜感开始被系统抽税，且过去没救下的人不再只是背景损失，而是会回来追债的现实后果。",
        "odd_romance": "看见旁白税开始自动扣款时，两人先挤在一张床边翻数据，翻着翻着就抱在一起，像只要身体还暖着，真实性就没被全抽走。",
        "even_romance": "被回流样本情侣逼到眼前时，他们没有先分工吵架，而是先在门后很用力地抱了一下，再各自出去扛那场后果。",
        "odd_heat_focus": "被抽税后的床边贴抱和压惊",
        "even_heat_focus": "危险回流前的门后用力拥抱",
    },
    19: {
        "world_arc": "看似失败的旧项目成了最难收编的现实节点，多城系统则想把恋爱抽成战争资源",
        "relationship_arc": "他们把共同生活和共同判断都变成抵抗战时调用的防线",
        "sweetness_arc": "在奔波和动员里还会顺手照顾对方、抢着给彼此留位置和留饭",
        "pair_payoff": "旧项目终于从失败记录变成反收编节点，而多城联动也把他们的关系正式抬到了战争资源的级别。",
        "odd_romance": "一路回看旧项目时，陆既明总能先一步替她记住那些名字，秦荔则嘴上嫌他啰嗦，身体还是很诚实地往他怀里靠，让‘失败没白废’先变成两个人的软着陆。",
        "even_romance": "发现多城系统想把恋爱当战争资源后，两人在夜车上挤着坐到一起，边商量边把对方手指扣住，像在说谁都别想把这段关系从他们自己手里征走。",
        "odd_heat_focus": "翻旧项目时的靠肩安顿和心软",
        "even_heat_focus": "夜车上的扣指和防征用式贴近",
    },
    20: {
        "world_arc": "旧弹幕正在重剪时间线，而所有偏转都在向未来共同生活借债",
        "relationship_arc": "他们第一次正面讨论要不要为了彼此少救一点世界，把余生当成明确议题摆上桌",
        "sweetness_arc": "夜里抱着谈未来，不再只谈眼前这次怎么赢",
        "pair_payoff": "第五卷卷末明确了最大的债主不是机构，而是他们未来本该一起过上的平常日子。",
        "odd_romance": "旧弹幕按新顺序复活时，两人窝在一起一条条翻，翻到发毛就互相靠近，像靠得更紧一点才能把被重剪的时间线重新认回自己手里。",
        "even_romance": "算出未来债务那晚，他们抱着躺到天亮，第一次认真谈要不要为了彼此少救一点世界，越说越像把‘余生’从抽象概念抱回了身体里。",
        "odd_heat_focus": "重剪时间线时的贴靠和互相认领",
        "even_heat_focus": "抱着谈未来到天亮的卷末余温",
    },
    21: {
        "world_arc": "旁白盲区彻底失守，未来标题开始倒着砸向现在",
        "relationship_arc": "他们从依赖盲区保护，推进到主动在被观看中重新造出只属于彼此的动作和暗号",
        "sweetness_arc": "被看见时也要贴、被打断后还要继续的危险甜感",
        "pair_payoff": "第六卷开局就打碎了盲区安全感，逼他们在更危险的观看里重新发明亲密。",
        "odd_romance": "第一次在接吻时听见旁白之后，他们先是一僵，随即谁都没退，反而把那个吻继续做完，像要当场把‘被看见也不让你们拆’写给外层看。",
        "even_romance": "明天的标题砸回今天时，两人贴着电梯壁分辨哪一句是陷阱、哪一句能用，明明危险得要命，却还是会在停顿里偷偷碰一下彼此嘴角。",
        "odd_heat_focus": "被旁白闯入时仍继续的危险接吻",
        "even_heat_focus": "未来标题压顶时的电梯贴近和偷碰",
    },
    22: {
        "world_arc": "旁白税开始侵入私密记忆，而秦荔的旧文正被证明是提前排练现在的脚本",
        "relationship_arc": "他们把‘重新记住彼此’从安慰动作推进成抵抗删除的日常义务",
        "sweetness_arc": "失忆后的再靠近、把旧细节重新讲给彼此听的温柔和心酸",
        "pair_payoff": "记忆不再安全，而秦荔也确认自己曾被借手排练现在，这使两人的亲密不得不承担记忆锚点功能。",
        "odd_romance": "陆既明忘掉那个只属于他们的细节后，秦荔一边气一边把人抱住，靠在他耳边把那件小事重新讲一遍，像用体温替这段记忆压回去。",
        "even_romance": "秦荔在旧文里看见现在后，陆既明陪她一页页对照，看到发冷处就把她往怀里拢一点，让‘原来你早被借手写过’不至于把人整个冻住。",
        "odd_heat_focus": "失忆后的抱着重讲和重新记住",
        "even_heat_focus": "对照旧文时的怀里取暖和心慌确认",
    },
    23: {
        "world_arc": "观测者也处在被看体系里，而所谓读者其实被分层安置在不同观察壳中",
        "relationship_arc": "他们从只对抗眼前机构升级成一起面对更高外部，并重新定义谁算见证者、谁算共谋者",
        "sweetness_arc": "在更大恐惧下依旧先贴近再开战、先确认你在再谈世界",
        "pair_payoff": "中层观测站失去神秘光环，真正的外部结构开始显影，而两人也把战斗对象抬到了整套分层观看机制。",
        "odd_romance": "听见观测者承认自己也在被看后，他们第一反应不是继续盘问，而是下意识先碰到对方，像要确认至少这一层真实还在彼此手里。",
        "even_romance": "确认读者也在壳中之后，两人坐在深夜空荡的走廊里肩贴着肩讲了很久，最后又亲了一下，像在给‘见证’重新定义一个不被壳子偷走的版本。",
        "odd_heat_focus": "更大外部压下时的先碰到彼此",
        "even_heat_focus": "重新定义见证后的走廊贴肩和轻吻",
    },
    24: {
        "world_arc": "他们不是唯一主角，而作者权限已经亲自下场逼他们在世界和余生之间二选一",
        "relationship_arc": "他们第一次明确约定谁都不准替谁做牺牲决定，把选择彼此变成规则底线",
        "sweetness_arc": "看完坏结局后先抱紧，不让对方一个人扛高阶选择",
        "pair_payoff": "第六卷卷末把终极敌人从系统代理升级成作者权限本体，也把‘谁都不准替谁牺牲’写成终局前的关系硬约束。",
        "odd_romance": "看见不同城市和阶段的主角群像后，两人既酸又清醒，秦荔先去拽他袖子，陆既明则顺势把她整个圈进怀里，像在群像里先把这一版主角抱稳。",
        "even_romance": "看完作者权限给出的坏结局后，他们没有先谈战略，而是先把对方抱紧，明确说谁都不准替谁做牺牲决定，再去拆那道假选择。",
        "odd_heat_focus": "群像冲击后的拽袖抱稳",
        "even_heat_focus": "坏结局面前的抱紧和禁止牺牲约定",
    },
    25: {
        "world_arc": "婚约模板本质上来自宇宙级治理协议，而终极敌人想把所有爱情压成统一句法",
        "relationship_arc": "他们把‘不被写成同一句话’推进成终局前的首要共同目标",
        "sweetness_arc": "面对终极模板时更想确认彼此说话方式、身体节奏和生活用语都还属于自己",
        "pair_payoff": "第七卷开局就把婚约荒唐感抬成宇宙级协议问题，也明确了终极敌人的目标是消灭主动偏爱。",
        "odd_romance": "追到婚约模板源头后，两人一边拆协议一边顺手纠正对方说话里的习惯词，像在最宏大的模板面前先把彼此的口头禅和身体节奏护住。",
        "even_romance": "看清有人想把所有爱情压成同一句话后，他们故意在最私密的交流里把彼此说话方式弄得更乱、更熟、更像只有对方才听得懂的版本。",
        "odd_heat_focus": "拆协议时护住彼此说话和节奏",
        "even_heat_focus": "反统一句法的私密熟话和贴近",
    },
    26: {
        "world_arc": "反派真正想消灭的是不可预测的主动偏爱，而秦荔决定把亲密直接写进篡改世界的方案",
        "relationship_arc": "他们从共同抵抗升级成把欲望、判断和夺权动作直接捆在一起",
        "sweetness_arc": "先亲再开战、在最硬的战略节点还优先选彼此",
        "pair_payoff": "反派目的明牌后，他们反而把亲密推进成正式战术，拒绝接受夺权就要先放下爱的逻辑。",
        "odd_romance": "对手明牌那一刻，他们都火得很清醒，回去后先狠狠干了一场，把那些‘你凭什么叫这叫稳定’的火气全烧成只属于他们的版本。",
        "even_romance": "秦荔决定先亲他再去篡改世界，于是她真的这么做了，把亲密直接写进方案里，让每一次贴近都同时承担战术和誓言功能。",
        "odd_heat_focus": "反派明牌后的高热泄火和确认",
        "even_heat_focus": "先亲再开战的战术性高热",
    },
    27: {
        "world_arc": "稳定世界的不是抽象爱，而是不断重复且坚定的选择彼此，旧配角则带着答案回场",
        "relationship_arc": "他们把‘选择彼此’从情感判断推进成可以写进模型和调度的硬规则",
        "sweetness_arc": "把每次主动选择都写得很具体，包括先找谁、先抱谁、先信谁",
        "pair_payoff": "前期伏笔大规模回流的同时，终局核心公式也被补完为‘选择彼此’而非抽象爱。",
        "odd_romance": "陆既明补模型时，秦荔一直坐在他手边陪着，时不时伸手碰他一下，像把‘选择’这件事先写成手边一直有你。",
        "even_romance": "旧配角带着答案回场时，两人一边接回每个人守住的碎片，一边下意识继续先看向彼此，越发确认这套世界真正学不走的是他们每次先选对方的习惯。",
        "odd_heat_focus": "补模型时手边一直有对方",
        "even_heat_focus": "众人回场中的先看向彼此和先选彼此",
    },
    28: {
        "world_arc": "秦荔被陆既明写进最终公式成为共同书写者，而外层观察接口终于能被直接夺取",
        "relationship_arc": "他们从一个解释世界一个点火世界，彻底升级成共同书写、共同夺权的一对",
        "sweetness_arc": "说得更直白的承诺和先上床再做战略复盘的终局亲密",
        "pair_payoff": "第七卷卷末，他们同时拿到共同书写的公式位置和外层接口位置，真正进入夺权前夜。",
        "odd_romance": "陆既明把‘你’补进最终公式后，秦荔先凑过去亲他，像要把这句最重要的数学修正落成一次身体签字。",
        "even_romance": "摸到外层接口前夜，两人先把门关上狠狠干一场，再抱着做战略复盘，让欲望、誓言和夺权坐到同一张床边。",
        "odd_heat_focus": "把你写进公式后的身体签字",
        "even_heat_focus": "夺权前夜的高热床边复盘",
    },
    29: {
        "world_arc": "第一章那句异常文案是未来内部回传的锚点，而邀请函从一开始就写给未来持有局部写作权的秦荔",
        "relationship_arc": "他们终于把起点线索和未来自己的行动接上，关系从当下热恋升级成跨时间协作",
        "sweetness_arc": "一起解自己留给自己的谜时仍旧习惯性贴着、抢话、先安抚再推理",
        "pair_payoff": "终局写回的起点和收件人都被解开，他们确认最早的异常一直来自未来版本的自己。",
        "odd_romance": "知道第一章那句文案其实来自未来的他们后，两人先安静抱了一会儿，像在替过去和未来同时抱住这一版自己。",
        "even_romance": "拆邀请函时他们肩挨着肩一句句读，读到收件人其实是未来秦荔时，陆既明先去碰她手背，让她知道现在这版也还在这里。",
        "odd_heat_focus": "起点回流后的安静拥抱",
        "even_heat_focus": "解邀请函时的肩挨肩和手背安抚",
    },
    30: {
        "world_arc": "所有样本情侣都在等一个被重新命名的结局，而深圳天幕正在逼他们公开选择真正写回路径",
        "relationship_arc": "他们开始把只属于自己的结局主动向更多具体的人敞开，而不是只保住两个人就算赢",
        "sweetness_arc": "看着失败稿和同居碎片时更舍不得松手，把日常直接当成终局筹码",
        "pair_payoff": "终局不再只关乎他们自己，样本情侣和失败稿一起把‘共同生活’抬成了必须被救回的公共结局。",
        "odd_romance": "看见所有样本情侣都在等一个结局时，他们没有先把门关上排外，反而牵得更紧，像终于准备把自己的幸福版本分给更多人。",
        "even_romance": "天幕播放出那些被删掉的同居碎片后，两人边看边抱紧，连最普通的床边对话都被抱得发烫，像终于知道自己真正要抢回来的是什么。",
        "odd_heat_focus": "牵着手面对所有样本情侣的回流",
        "even_heat_focus": "看见被删同居碎片时的发烫抱紧",
    },
    31: {
        "world_arc": "命名权必须被拆回具体的人，而共同生活终于能被写成新的宇宙硬规则",
        "relationship_arc": "他们把‘我们俩的日子’从私人愿望推进成能保护他人的新秩序底盘",
        "sweetness_arc": "把吃饭睡觉出门回家都认真写回规则，日常本身开始发热",
        "pair_payoff": "新的世界规则不再服务模板稳定性，而开始服务具体的人和具体的共同生活。",
        "odd_romance": "秦荔把命名权一点点拆回具体的人时，陆既明始终站在她一步之内，像在用身体说明‘你的写回从来不是一个人完成的’。",
        "even_romance": "当宇宙常数被改成共同生活概率时，两人几乎同时笑出来，下一秒就贴到一起，像终于看见连吃饭睡觉都能成为硬规则的未来。",
        "odd_heat_focus": "写回具体人时的一步之内相伴",
        "even_heat_focus": "共同生活常数写成后的贴近和笑意",
    },
    32: {
        "world_arc": "请继续被从命令改写成祝福，而终局写回的目标是把余生本身从剧本里抢回来",
        "relationship_arc": "他们不再只是在大战里相爱，而是把继续相爱本身写成结局之后仍会持续运行的规则",
        "sweetness_arc": "胜利后的余温、清晨、床、饭桌和自然开始过日子",
        "pair_payoff": "第一季收束时，世界不再要求下一章来许可他们相爱，他们把继续相爱和继续生活都写成了新默认值。",
        "odd_romance": "听见那句‘请继续’终于变成祝福后，两个人先是怔了一下，随后不约而同地靠过去抱住彼此，像终于把一路追来的命令抱成了温柔的人声。",
        "even_romance": "写回完成后，他们没有先去发表胜利宣言，而是先把人抱回床和清晨里，让醒来第一眼还能看见对方这件事成为新规则的第一条证明。",
        "odd_heat_focus": "祝福落地时的长抱和松气",
        "even_heat_focus": "终章清晨与余温里的日常收束",
    },
}


HOOK_TYPE_BY_REVERSAL = {
    "discovery": "reveal",
    "rule_upgrade": "rule",
    "identity_reveal": "reveal",
    "public_exposure": "threat",
    "trap_spring": "threat",
    "world_unlock": "reveal",
    "alliance_shift": "choice",
    "inversion": "reversal",
    "memory_shock": "reveal",
    "false_victory": "reversal",
    "character_turn": "choice",
    "infiltration": "reversal",
    "cost_reveal": "threat",
    "villain_reveal": "reveal",
    "payoff_chain": "payoff",
    "final_payoff": "payoff",
}

KEY_CHAPTER_VALIDATION: dict[int, dict[str, Any]] = {
    8: {
        "validation_cues": ["校准", "同时在场", "抱", "吻", "余温", "床"],
        "min_validation_hits": 3,
    },
    12: {
        "validation_cues": ["协议", "婚书", "床", "余温", "后腰", "衣料", "反命名"],
        "min_validation_hits": 4,
    },
    14: {
        "validation_cues": ["主样本", "拽", "压", "吻", "腰", "失真"],
        "min_validation_hits": 4,
    },
    28: {
        "validation_cues": ["标准件", "失控", "节奏", "吻", "呼吸", "后腰"],
        "min_validation_hits": 3,
    },
    40: {
        "validation_cues": ["账单", "天亮", "抱", "未来", "余温"],
        "min_validation_hits": 3,
    },
    48: {
        "validation_cues": ["结局", "抱", "牺牲", "选择", "余温"],
        "min_validation_hits": 3,
    },
    56: {
        "validation_cues": ["夺权", "誓言", "床", "欲望", "复盘", "余温"],
        "min_validation_hits": 3,
    },
    64: {
        "validation_cues": ["清晨", "抱", "余温", "饭桌", "醒来"],
        "min_validation_hits": 3,
    },
}

MUST_FULL_SEX_CHAPTERS = {8, 12, 52, 56}
OPTIONAL_FULL_SEX_CHAPTERS = {14, 18, 19, 22, 25, 28, 31, 38, 41, 49, 51, 55}
AFTERGLOW_ONLY_CHAPTERS = {13, 21, 24, 26, 30, 34, 35, 39, 40, 44, 48, 60, 62, 63, 64}

VOLUME_SWEETNESS_PACKS: dict[int, list[str]] = {
    1: [
        "工作流发糖：顺毛、投喂、贴肩合稿、忙里偷亲。",
        "记忆点：一转头就能接住对方情绪，边救火边谈恋爱。",
    ],
    2: [
        "公开偏心：人前站队、人后贴近、整座城替他们打掩护。",
        "记忆点：公开认领之后，私下更像多年熟人情侣而不是新鲜热恋。",
    ],
    3: [
        "反模板甜：被模仿后更确认私话、暗号、姓名和身体习惯。",
        "记忆点：只有你知道我什么时候真炸毛、什么时候只是嘴硬。",
    ],
    4: [
        "共谋甜：假婚约、档案潜查、暗号配合、拿吻当反制接口。",
        "记忆点：假的制度压不住真的熟悉感和联盟感。",
    ],
    5: [
        "生活甜：异城短住、洗澡后复盘、夜车扣指、疲惫里先抱一下。",
        "记忆点：越狼狈越像已经过了很多年日子。",
    ],
    6: [
        "守住彼此甜：失忆后重新记住、被观看后重新发明暗号、先安抚再推理。",
        "记忆点：就算私密被看见，属于他们的熟悉感也会重新长出来。",
    ],
    7: [
        "硬核甜辣：先亲再开战、私人誓言、把欲望和选择直接写成夺权动作。",
        "记忆点：你不是软肋，而是共同作者。",
    ],
    8: [
        "余生甜：清晨、赖床、饭桌、洗漱、出门回家、自然开始共同生活。",
        "记忆点：赢完之后，他们第一反应还是贴过去过日子。",
    ],
}

CHAPTER_SWEETNESS_MUST_LAND: dict[int, str] = {
    1: "顺毛投喂时顺手把她那版文案一起改完。",
    2: "挡桌角后扣手偷亲，再一起扛完爆红余波。",
    3: "把“七秒初吻”从旧笑话写回现在的熟练偏心。",
    4: "当众勾后颈接吻，把偏爱直接写进热搜现场。",
    5: "焐手护腰后用坏吻把第三视角钓出来。",
    6: "桌下勾手到电梯口狠吻，把招安会改成双人秀。",
    7: "坐腿控评、共盯后台、边工作边试探性接吻。",
    8: "校准从牵手抱紧推到关门后的高热确认。",
    9: "楼梯间先嘴硬再亲到他眼镜都歪。",
    10: "黑布、耳返、倒计时里差点擦枪走火。",
    11: "安全通道长抱，把“整座城都知道你是我的”抱实。",
    12: "回家狠狠干一场，并写足反命名后的照料与松气。",
    13: "故意把模板动作做给彼此看，再让真实反应反杀模板。",
    14: "把“你不能把他从我这拆走”写成身体上的反制。",
    15: "真相崩塌后先抱住压惊，再谈怎么继续。",
    16: "看见多版本地图后先盖章只认眼前这一版。",
    17: "看旧作文时把她搂回怀里、亲额角、陪她把旧纸页看完。",
    18: "潜入后台时贴着分工、压低嗓子耳语，出门后短促落印。",
    19: "被更像的样本激怒后，用熟到骨子的反应确认“你还是你”。",
    20: "把秦荔的具体名字先写进论文，再写进一个很长的吻。",
    21: "翻旧名单时贴肩复盘，让“被普通人护住”带出更深偏爱。",
    22: "识破产业链后抱着复盘到发热，把私话磨得更像暗号。",
    23: "躲起来抱一会儿，第一次让世界替他们托一口气。",
    24: "真相摊开时先抱紧，不让最冷的一层真相直接落在她身上。",
    25: "档案室外压着声音对计划，顺手碰手腕确认共谋。",
    26: "重命名城市后回家补全那些被模板抹平的生活细节。",
    27: "把洗漱、宵夜、抢被子写成真正能稳住人的共同生活。",
    28: "故意狠狠干亲一回，让每个失控都只属于他们的节奏。",
    29: "难做决定时先抱着商量，不让任何一方独自扛。",
    30: "模型裂开时用带笑的安抚吻把“我们一起补”写实。",
    31: "假婚约表演里眼神对暗号、身体却越来越像真的熟人。",
    32: "草稿页真相后先把彼此按回怀里，再讲下一步战争计划。",
    33: "陌生城市里也自然挤同一张沙发、共用同一条毛巾。",
    34: "洗澡后抱着复盘到深夜，把代价当成两个人一起扛。",
    35: "床边挤着翻账单，先压惊再谈收场。",
    36: "样本情侣回流前先在门后很用力地抱一下。",
    37: "回看旧项目时靠肩安顿，让“失败没白废”先有软着陆。",
    38: "夜车上扣住手指，说谁都别想把这段关系征走。",
    39: "一条条重看旧弹幕时越靠越近，把时间线重新认回来。",
    40: "抱着谈未来到天亮，第一次认真谈余生不是抽象词。",
    41: "听见旁白后谁都不退，反而把那个吻继续做完。",
    42: "电梯停顿里偷碰嘴角，确认明天砸下来前你还在这。",
    43: "抱着把丢掉的那件小事重新讲给他听。",
    44: "一页页对照旧文时把她往怀里拢，先取暖再读真相。",
    45: "知道更大外部存在后先碰到彼此，再继续开战。",
    46: "深夜走廊肩贴肩重定义“见证”，最后落一个轻吻。",
    47: "在群像冲击里先把这一版彼此抱稳。",
    48: "看完坏结局先抱紧并说清谁都不准替谁牺牲。",
    49: "拆协议时逐条改成只对彼此有效的私人誓言。",
    50: "故意把说话方式、生活用语和身体节奏弄得更像只有对方懂。",
    51: "先把火气和清醒都烧成只属于他们的版本，再回到战场。",
    52: "真正先亲他，再去篡改世界。",
    53: "补模型时手边一直有对方，碰一下就是一次选择。",
    54: "人都回来了，他们还是会下意识先看向彼此。",
    55: "他把“你”写进公式后，要用一个带签字意味的吻落章。",
    56: "门关上狠狠干一场，再抱着做夺权前夜复盘。",
    57: "知道起点来自未来后，先安静抱住这一版彼此。",
    58: "肩挨肩拆邀请函，读到关键处先碰手背安抚。",
    59: "牵着手面对所有样本情侣，不再只保两个人。",
    60: "看见被删同居碎片时抱得发烫，终于知道要抢回什么。",
    61: "她写回具体人时，他始终站在一步之内。",
    62: "看见共同生活变成宇宙常数后先笑再贴近。",
    63: "把“请继续”从命令抱成温柔的人声。",
    64: "醒来第一眼还能看见对方，然后自然开始过日子。",
}


def _chapter_number(chapter: dict[str, Any]) -> int:
    return int(chapter.get("chapter_number") or chapter.get("number") or 0)


def _pair_index(chapter_number: int) -> int:
    return (chapter_number + 1) // 2


def _chapter_side(chapter_number: int) -> str:
    return "odd" if chapter_number % 2 == 1 else "even"


def _volume_checkpoint(chapter_number: int) -> str:
    offset = chapter_number % 8
    if offset == 0:
        return "required"
    if offset == 7:
        return "approach"
    return "carry"


def _default_intimacy_cues(level: int) -> list[str]:
    if level >= 5:
        return ["床", "被子", "余温", "呼吸", "掌心", "后腰", "欲望", "吻"]
    if level >= 4:
        return ["床", "床边", "被子", "余温", "呼吸", "掌心", "后腰", "衣料", "吻"]
    if level >= 3:
        return ["吻", "呼吸", "腰", "后腰", "掌心", "腿", "贴", "压近"]
    if level >= 2:
        return ["抱", "亲", "吻", "手", "腰", "腿", "靠", "贴"]
    return ["手", "肩", "靠", "贴"]


def _intimacy_scale_map(plan: dict[str, Any]) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for item in (plan.get("writing_system", {}) or {}).get("intimacy_scale", []) or []:
        level = int(item.get("level") or 0)
        if level:
            result[level] = dict(item)
    return result


def _progression_for_chapter(plan: dict[str, Any], chapter_number: int) -> dict[str, Any]:
    for item in (plan.get("writing_system", {}) or {}).get("intimacy_progression", []) or []:
        start = int(item.get("chapter_start") or 0)
        end = int(item.get("chapter_end") or 0)
        if start and end and start <= chapter_number <= end:
            return dict(item)
    return {}


def _build_world_progress(meta: dict[str, str], side: str) -> str:
    if side == "odd":
        return f"先把{meta['world_arc']}的线索掀出来，让异常从直觉变成可追查对象。"
    return f"正式坐实{meta['world_arc']}，让世界规则在这一章留下不可逆后果。"


def _build_relationship_progress(meta: dict[str, str], side: str) -> str:
    if side == "odd":
        return f"先把{meta['relationship_arc']}推到台面，逼他们把亲密从熟练反应升级成主动判断。"
    return f"正式坐实{meta['relationship_arc']}，让这段关系在公共层、规则层或生活层留下后果。"


def _chapter_heat_focus(meta: dict[str, str], side: str) -> str:
    return meta["odd_heat_focus"] if side == "odd" else meta["even_heat_focus"]


def _chapter_execution_mode(chapter_number: int) -> str:
    if chapter_number in MUST_FULL_SEX_CHAPTERS:
        return "must_full_sex"
    if chapter_number in OPTIONAL_FULL_SEX_CHAPTERS:
        return "optional_full_sex"
    if chapter_number in AFTERGLOW_ONLY_CHAPTERS:
        return "afterglow_only"
    return "no_full_sex"


def _boundary_note_for_mode(execution_mode: str) -> str:
    if execution_mode == "must_full_sex":
        return "本章必须正面落成完整高热戏，且要写连续动作、判断变化和事后余温。"
    if execution_mode == "optional_full_sex":
        return "本章可以顺势写到床，但不是刚性 KPI；不落床时也要把高热与未尽感写满。"
    if execution_mode == "afterglow_only":
        return "本章重点是余温、复盘和生活感，不另起新的完整床戏。"
    return "本章不落完整床戏，重点把热度压进身体距离、动作和未尽后劲。"


def _afterglow_requirement_for_mode(execution_mode: str) -> str:
    if execution_mode == "must_full_sex":
        return "必须写足事后余温、照料或复盘，不允许高热一落地就切走。"
    if execution_mode == "optional_full_sex":
        return "无论是否落床，都要写清事后照料、压惊或延续到下一章的未尽感。"
    if execution_mode == "afterglow_only":
        return "必须落到床边、洗澡后、清晨、复盘或长抱其中一种余温场景。"
    return "至少保留一个能延续到下一章的热度余波或生活细节。"


def _build_on_page_expectation(execution_mode: str, heat_focus: str, must_land: str) -> str:
    if execution_mode == "must_full_sex":
        return (
            f"必须把{heat_focus}正面写成完整高热戏，包含连续动作、身体反应和事后余温；"
            f"并让“{must_land}”真正改变关系和局势。"
        )
    if execution_mode == "optional_full_sex":
        return (
            f"重点写{heat_focus}，可以顺着情节自然写到上床，但不是硬性任务；"
            f"核心是把“{must_land}”写成会改局的高热推进。"
        )
    if execution_mode == "afterglow_only":
        return (
            f"重点写{heat_focus}的余温、床边/洗澡后/清晨/复盘，不另起完整新床戏；"
            f"核心是把“{must_land}”写成持续发热的生活感。"
        )
    return f"重点写{heat_focus}，把“{must_land}”落到具体动作、呼吸变化和未尽后劲上，不落完整床戏。"


def _build_sweetness_target(meta: dict[str, str], chapter_number: int, side: str) -> dict[str, str]:
    heat_focus = _chapter_heat_focus(meta, side)
    next_chapter_number = chapter_number + 1
    next_focus = "结尾之后仍会继续的日常"
    if next_chapter_number <= 64:
        next_meta = PAIR_META[_pair_index(next_chapter_number)]
        next_focus = _chapter_heat_focus(next_meta, _chapter_side(next_chapter_number))
    return {
        "core_mode": heat_focus,
        "must_land": CHAPTER_SWEETNESS_MUST_LAND.get(chapter_number, heat_focus),
        "novelty_rule": (
            f"从第一章就把主糖点落在“{heat_focus}”，不要把熟悉感写成空泛背景。"
            if chapter_number == 1
            else f"不要只重复上一章的主糖点；这章的主甜法必须落在“{heat_focus}”。"
        ),
        "carryover": f"把这份甜感余波带到下一章的“{next_focus}”。",
    }


def _build_sweetness_progress(chapter_number: int, sweetness_target: dict[str, str], side: str) -> str:
    must_land = sweetness_target.get("must_land") or sweetness_target.get("core_mode") or "一个具体甜点"
    heat_focus = sweetness_target.get("core_mode") or "本章主甜法"
    if side == "odd":
        return f"本章甜蜜升级：把“{must_land}”写成能被读者记住的第一次点火，让{heat_focus}开始真正改局。"
    return f"本章甜蜜升级：把“{must_land}”真正兑现，让{heat_focus}从点火变成关系默认动作。"


def _build_intimacy_target(
    plan: dict[str, Any],
    chapter_number: int,
    heat_focus: str,
    relationship_progress: str,
    pair_payoff: str,
    sweetness_target: dict[str, str],
) -> dict[str, Any]:
    progression = _progression_for_chapter(plan, chapter_number)
    level = int(progression.get("default_level") or 1)
    execution_mode = _chapter_execution_mode(chapter_number)
    if execution_mode == "must_full_sex":
        level = max(level, 4)
    must_land = sweetness_target.get("must_land") or heat_focus
    return {
        "level": level,
        "label": heat_focus,
        "execution_mode": execution_mode,
        "boundary_note": _boundary_note_for_mode(execution_mode),
        "scene_payload": [heat_focus, must_land],
        "afterglow_requirement": _afterglow_requirement_for_mode(execution_mode),
        "on_page_expectation": _build_on_page_expectation(execution_mode, heat_focus, must_land),
        "function": f"让这场亲密直接推动：{relationship_progress}",
        "required_outcome": pair_payoff if chapter_number % 2 == 0 else relationship_progress,
    }


def _build_beats(chapter: dict[str, Any], world_progress: str, relationship_progress: str, romance_beat: str) -> list[str]:
    return [
        f"开场立刻进入这个现场：{chapter.get('summary', '').strip()}",
        f"把外部推进和世界升级压实：{chapter.get('key_conflict', '').strip()}；{world_progress}",
        f"把关系和甜度写成具体动作：{relationship_progress}；{romance_beat}",
        f"章尾必须咬住这个钩子：{chapter.get('hook', '').strip()}",
    ]


def upgrade_plan(plan: dict[str, Any]) -> dict[str, Any]:
    plan["version"] = 6
    plan["updated_at"] = "2026-03-20T23:30:00+08:00"

    writing_notes = plan.get("writing_notes", {}) or {}
    writing_notes["narrative_rule"] = (
        "章节默认以现实任务、关系甜度、世界规则三线协同推进；允许单章偏重其中两线，"
        "但双章合起来必须把三线都往前推。整书层面每 2 章必须出现一次不可逆转折，每 8 章必须完成一次世界层级跃迁。"
    )
    writing_notes["sweetness_balance_rule"] = (
        "世界观的展开不能冲淡感情的甜蜜；越是大信息量章节，越要给出一个可记住的恋爱动作、照料细节或共同生活片段。"
    )
    writing_notes["physical_intimacy_rule"] = (
        "尺度由章节功能决定，不搞机械配额。整书要持续有甜感和身体热度，每两章至少留下一个可回味的亲密记忆点，"
        "每卷至少有一次明确升级场；L4/L5 代表系统承载能力，不代表每章都必须落床戏。"
    )
    writing_notes["emotional_upgrade_rule"] = (
        "甜蜜感必须持续升级，速度不低于肉体亲密升级。优先升级偏心、共犯、照料、共同生活、私密暗号和‘先选彼此’，不用机械凑糖。"
    )
    writing_notes["system_execution_rule"] = (
        "所有节奏要求都要进入可执行结构：下一章生成时必须显式带入双章转折检查点、卷末扩层检查点、本章亲密等级、"
        "本章亲密执行模式和本章甜点设计，并同时带入 world_progress、relationship_progress、sweetness_progress、"
        "sweetness_target、turn_role、pair_payoff、volume_upgrade_checkpoint、hook_type。"
    )
    writing_notes["sweetness_checklist"] = [
        "本章至少落一个具体甜点：动作、玩笑、照料或共同生活细节。",
        "大信息量章节也要保留可回味的恋爱动作，不能只剩设定解释。",
        "不写完整床戏时，也要写清余波、照料或生活感。",
        "相邻两章主糖点不重复，不靠机械抱抱亲亲续命。",
        "至少有一个甜点能把热度带到下一章。",
        "世界线推进不能把两人写成只会解释设定的搭档。",
    ]
    plan["writing_notes"] = writing_notes

    writing_system = plan.get("writing_system", {}) or {}
    writing_system["romance_heat_profile"] = (
        "高糖亲密为默认状态。允许拥抱、牵手、接吻、贴靠、同居日常、洗澡后复盘、床边余温与随情节升级的明确性张力；"
        "系统始终具备写高热戏的能力，但是否正面落床由章节 execution_mode 决定。"
    )
    writing_system["intimacy_scale"] = [
        {
            "level": 1,
            "label": "熟人式贴靠",
            "page_expectation": "写到贴肩、拉手、搂一下、喂食、赖着不走，重点是熟悉感和偏爱。",
            "function": "证明他们是多年热恋，不是刚起步的暧昧。",
        },
        {
            "level": 2,
            "label": "明确升温",
            "page_expectation": "写清偷亲、长一点的吻、坐腿、门后压近、手掌和腰背的动作。",
            "function": "让甜感直接参与现场推进，而不是只做装饰。",
        },
        {
            "level": 3,
            "label": "欲望上桌",
            "page_expectation": "写清更明显的欲望、呼吸变化、衣料摩擦、床边或车里的升温和被打断后的余波。",
            "function": "让欲望本身成为做决定、试规则或反收编的推进器。",
        },
        {
            "level": 4,
            "label": "关门后的正面床戏",
            "page_expectation": "系统可承载正面床戏、做爱推进和事后余温；是否落地由章节 execution_mode 决定，不默认章章上床。",
            "function": "把亲密升级写成关系改写和世界规则对冲，而不是单纯发福利。",
        },
        {
            "level": 5,
            "label": "欲望与夺权绑定",
            "page_expectation": "系统可承载更直白的高热场面与更强的欲望/权力绑定；只在关键章节落地，不机械泛滥。",
            "function": "让亲密成为终局权力斗争的一部分。",
        },
    ]
    writing_system["intimacy_progression"] = [
        {
            "chapter_start": 1,
            "chapter_end": 8,
            "default_level": 3,
            "cap_level": 4,
            "on_page_expectation": "前半段迅速建立熟人式黏糊和欲望上桌，卷末必须落下一次真正的高热关门戏；除了卷末，不默认每章都正面落床。",
            "must_land": "前 8 章内必须让读者明确感到：他们的亲密不只是接口，而且已经快到会先把门关上再谈规则。",
        },
        {
            "chapter_start": 9,
            "chapter_end": 16,
            "default_level": 4,
            "cap_level": 4,
            "on_page_expectation": "维持可写正面床戏的承载力，但是否落床由章节 execution_mode 决定；重点升级公开性、测试环境和事后反写。",
            "must_land": "第二卷必须把公开认领、私下高热和反命名后的余温同时推上台面。",
        },
        {
            "chapter_start": 17,
            "chapter_end": 24,
            "default_level": 4,
            "cap_level": 4,
            "on_page_expectation": "亲密戏主要承担安抚、确认和反模板功能，多写复盘、回家后的贴靠和被模仿后更确认彼此，不要求章章正面床戏。",
            "must_land": "被模仿越厉害，他们越要用具体身体记忆把关系写实。",
        },
        {
            "chapter_start": 25,
            "chapter_end": 32,
            "default_level": 4,
            "cap_level": 4,
            "on_page_expectation": "允许部分章节落成完整高热戏，但重点是把欲望和共谋写进反制接口，不搞平均摊开的床戏分布。",
            "must_land": "第四卷必须写出真假绑定交错下的熟悉感和联盟感。",
        },
        {
            "chapter_start": 33,
            "chapter_end": 40,
            "default_level": 4,
            "cap_level": 4,
            "on_page_expectation": "中高热场面与同居琐碎并写，但主轴是老夫老妻式照顾、异城短住和疲惫里的黏糊，不默认继续抬床戏密度。",
            "must_land": "第五卷要让疲惫、追账和热恋同框。",
        },
        {
            "chapter_start": 41,
            "chapter_end": 48,
            "default_level": 4,
            "cap_level": 5,
            "on_page_expectation": "高热场面要承担记忆缺口、被观看与重新确认彼此的压力；更多章节用余波、再记住和守住彼此来发热。",
            "must_land": "第六卷的亲密戏要发烫，也要带疼，但不能写成机械连续床戏。",
        },
        {
            "chapter_start": 49,
            "chapter_end": 56,
            "default_level": 5,
            "cap_level": 5,
            "on_page_expectation": "只有第七卷把做爱、誓言、协议和夺权真正绑死；大尺度集中在关键章节，不向全卷平均摊开。",
            "must_land": "第七卷必须写出先亲再开战、先上床再定战略的硬核亲密。",
        },
        {
            "chapter_start": 57,
            "chapter_end": 64,
            "default_level": 4,
            "cap_level": 4,
            "on_page_expectation": "回到共同生活的可持续亲密，重点重写清晨、床边、饭桌、通勤和洗漱台前的身体熟悉感，不再默认继续落新床戏。",
            "must_land": "终局必须让余生具体到可触摸的生活。",
        },
    ]
    writing_system["sweetness_system"] = {
        "sweetness_axes": ["偏心", "共犯", "照料", "生活", "玩笑", "公开认领", "私密暗号", "事后安抚", "未来感", "共同生活"],
        "sweetness_density_rule": "默认每章至少一个具体甜点，每两章至少一次可回味的高记忆点，每卷至少一次成体系的甜蜜升级场；不搞机械配额。",
        "sweetness_scene_types": [
            "工作流发糖",
            "危险场景发糖",
            "同居感发糖",
            "嘴硬手软发糖",
            "余温发糖",
            "共谋发糖",
            "公开偏心发糖",
            "笨拙照料发糖",
        ],
        "sweetness_escalation_map": "从熟人式黏糊一路升级到共同生活即规则本身。",
    }
    execution_blueprint = writing_system.get("execution_blueprint", {}) or {}
    execution_blueprint["required_chapter_fields"] = [
        "summary",
        "key_conflict",
        "romance_beat",
        "beats",
        "intimacy_target",
        "sweetness_target",
        "seed_threads",
        "payoff_threads",
        "world_progress",
        "relationship_progress",
        "sweetness_progress",
        "turn_role",
        "pair_payoff",
        "volume_upgrade_checkpoint",
        "hook_type",
        "reversal_type",
        "world_layer",
        "hook",
    ]
    execution_blueprint["chapter_beat_contract"] = [
        "第 1 beat 负责具体开场现场，必须把动作、场景或异常立刻推上桌。",
        "第 2 beat 负责现实任务或外部局势推进，并写清本章世界升级。",
        "第 3 beat 负责关系/亲密/甜度升级，且必须改变局面。",
        "第 4 beat 负责章尾钩子，不允许收平。",
    ]
    execution_blueprint["turn_role_values"] = {
        "odd": "ignite",
        "even": "detonate",
    }
    execution_blueprint["volume_checkpoint_values"] = {
        "carry": "本章继续为卷末扩层蓄压。",
        "approach": "本章必须逼近卷末扩层，不允许旁支化。",
        "required": "本章必须完成卷末扩层、关系升级和亲密升级三线并发。",
    }
    execution_blueprint["sweetness_upgrade_cycle"] = {
        "cycle_size": 2,
        "rule": "默认每章至少一个具体甜点，每两章至少一次可回味高记忆点；相邻两章主糖点不重复。",
    }
    writing_system["execution_blueprint"] = execution_blueprint
    plan["writing_system"] = writing_system

    relationship_mainline = plan.get("relationship_mainline", {}) or {}
    relationship_mainline["sweetness_quota"] = (
        "默认每章至少一个具体甜点，每两章至少一次高记忆点，每卷至少一次成体系的甜蜜升级场；不搞机械凑糖。"
    )
    relationship_mainline["sweetness_density_rule"] = (
        "世界线越大，越要把甜落回具体动作、照料、玩笑和共同生活；高热戏不是均匀分发，而是服务章节功能。"
    )
    plan["relationship_mainline"] = relationship_mainline

    story_bible = plan.get("story_bible", {}) or {}
    story_bible["relationship_rules"] = [
        "两人从初中开始恋爱，到 24 岁仍处于稳定热恋中。",
        "不允许用分手、误会、冷战、第三者、死亡虐点推进主线。",
        "高糖不是奖励，而是基础运行状态；默认每章至少给出一个可记住的恋爱动作、小习惯或共同生活细节。",
        "亲密升级速度不能慢于世界升级速度；前 8 章内必须落下一次真正的高热关门戏，但之后是否再落床戏由章节功能决定，不搞章章上床。",
        "甜蜜升级和肉体升级是两条线：偏心、共犯、照料、共同生活和“先选彼此”都要持续变强；世界观扩层不能冲淡甜味。",
    ]
    plan["story_bible"] = story_bible

    for volume in plan.get("volumes", []) or []:
        volume_number = int(volume.get("number") or 0)
        volume["sweetness_focus_pack"] = VOLUME_SWEETNESS_PACKS.get(volume_number, [])

    for chapter in plan.get("chapters", []) or []:
        chapter_number = _chapter_number(chapter)
        pair_meta = PAIR_META[_pair_index(chapter_number)]
        side = _chapter_side(chapter_number)
        heat_focus_key = "odd_heat_focus" if side == "odd" else "even_heat_focus"
        world_progress = _build_world_progress(pair_meta, side)
        relationship_progress = _build_relationship_progress(pair_meta, side)
        sweetness_target = _build_sweetness_target(pair_meta, chapter_number, side)
        sweetness_progress = _build_sweetness_progress(chapter_number, sweetness_target, side)
        romance_key = "odd_romance" if side == "odd" else "even_romance"
        romance_beat = str(chapter.get("romance_beat") or "").strip() or pair_meta[romance_key]

        chapter["world_progress"] = world_progress
        chapter["relationship_progress"] = relationship_progress
        chapter["sweetness_progress"] = sweetness_progress
        chapter["sweetness_target"] = sweetness_target
        chapter["turn_role"] = "ignite" if side == "odd" else "detonate"
        chapter["pair_payoff"] = pair_meta["pair_payoff"]
        chapter["volume_upgrade_checkpoint"] = _volume_checkpoint(chapter_number)
        chapter["hook_type"] = HOOK_TYPE_BY_REVERSAL.get(str(chapter.get("reversal_type") or "").strip(), "reversal")
        chapter["romance_beat"] = romance_beat

        existing_target = _build_intimacy_target(
            plan,
            chapter_number,
            pair_meta[heat_focus_key],
            relationship_progress,
            pair_meta["pair_payoff"],
            sweetness_target,
        )
        existing_target.pop("validation_cues", None)
        existing_target.pop("min_validation_hits", None)
        if chapter_number in KEY_CHAPTER_VALIDATION:
            existing_target.update(KEY_CHAPTER_VALIDATION[chapter_number])
        chapter["intimacy_target"] = existing_target

        existing_beats = chapter.get("beats") or []
        if not isinstance(existing_beats, list) or len(existing_beats) != 4:
            chapter["beats"] = _build_beats(chapter, world_progress, relationship_progress, romance_beat)

        if chapter_number == 64 and not chapter.get("seed_threads"):
            chapter["seed_threads"] = ["daily_life_after_writeback"]

    return plan


def main() -> int:
    plan = json.loads(PLAN_PATH.read_text())
    upgraded = upgrade_plan(plan)
    PLAN_PATH.write_text(json.dumps(upgraded, ensure_ascii=False, indent=2) + "\n")
    print(f"updated {PLAN_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
